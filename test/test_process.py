from plum.persistence.bundle import Bundle
from plum.process import Process, ProcessState
from plum.process_monitor import MONITOR
from plum.process_manager import ProcessManager
from plum.test_utils import DummyProcess, ExceptionProcess, TwoCheckpoint, \
    DummyProcessWithOutput, TEST_PROCESSES, ProcessSaver, check_process_against_snapshots, \
    WaitForSignalProcess, Barrier
from plum.test_utils import ProcessListenerTester
from plum.util import override
from plum.persistence.util import save_on_next_transition
from plum.wait_ons import wait_until
from util import TestCase


class ForgetToCallParent(Process):
    @classmethod
    def define(cls, spec):
        super(ForgetToCallParent, cls).define(spec)
        spec.input('forget_on', valid_type=str)

    @override
    def _run(self, forget_on):
        pass

    @override
    def on_start(self):
        if self.inputs.forget_on != 'start':
            super(ForgetToCallParent, self).on_start()

    @override
    def on_run(self):
        if self.inputs.forget_on != 'run':
            super(ForgetToCallParent, self).on_start()

    @override
    def on_fail(self):
        if self.inputs.forget_on != 'fail':
            super(ForgetToCallParent, self).on_start()

    @override
    def on_finish(self):
        if self.inputs.forget_on != 'finish':
            super(ForgetToCallParent, self).on_start()

    @override
    def on_stop(self):
        if self.inputs.forget_on != 'stop':
            super(ForgetToCallParent, self).on_start()


class TestProcess(TestCase):
    def setUp(self):
        super(TestProcess, self).setUp()

        self.events_tester = ProcessListenerTester()
        self.proc = DummyProcessWithOutput.new()
        self.proc.add_process_listener(self.events_tester)
        self.procman = ProcessManager()

    def tearDown(self):
        self.proc.remove_process_listener(self.events_tester)
        self.procman.abort_all(timeout=10.)
        self.assertEqual(self.procman.get_num_processes(), 0, "Failed to abort all processes")
        super(TestProcess, self).tearDown()

    def test_spec(self):
        """
        Check that the references to specs are doing the right thing...
        """
        dp = DummyProcess.new()
        self.assertIsNot(DummyProcess.spec(), Process.spec())
        self.assertIs(dp.spec(), DummyProcess.spec())

        class Proc(DummyProcess):
            pass

        self.assertIsNot(Proc.spec(), Process.spec())
        self.assertIsNot(Proc.spec(), DummyProcess.spec())
        p = Proc.new()
        self.assertIs(p.spec(), Proc.spec())

    def test_dynamic_inputs(self):
        class NoDynamic(Process):
            def _run(self, **kwargs):
                pass

        class WithDynamic(Process):
            @classmethod
            def define(cls, spec):
                super(WithDynamic, cls).define(spec)

                spec.dynamic_input()

            def _run(self, **kwargs):
                pass

        with self.assertRaises(ValueError):
            NoDynamic.run(a=5)
        WithDynamic.run(a=5)

    def test_inputs(self):
        class Proc(Process):
            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)
                spec.input('a')

            def _run(self, a):
                pass

        p = Proc.new({'a': 5})

        # Check that we can access the inputs after creating
        self.assertEqual(p.raw_inputs.a, 5)
        with self.assertRaises(AttributeError):
            p.raw_inputs.b

    def test_inputs_default(self):
        class Proc(DummyProcess):
            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)
                spec.input("input", default=5, required=False)

        # Supply a value
        p = Proc.new(inputs={'input': 2})
        self.assertEqual(p.inputs['input'], 2)

        # Don't supply, use default
        p = Proc.new()
        self.assertEqual(p.inputs['input'], 5)

    def test_run(self):
        p = DummyProcessWithOutput.new()
        p.play()

        self.assertTrue(p.has_finished())
        self.assertEqual(p.state, ProcessState.STOPPED)
        self.assertEqual(p.outputs, {'default': 5})

    def test_run_from_class(self):
        # Test running through class method
        results = DummyProcessWithOutput.run()
        self.assertEqual(results['default'], 5)

    def test_forget_to_call_parent(self):
        for event in ('start', 'run', 'finish', 'stop'):
            with self.assertRaises(AssertionError):
                ForgetToCallParent.run(forget_on=event)

    def test_pid(self):
        # Test auto generation of pid
        p = DummyProcessWithOutput.new()
        self.assertIsNotNone(p.pid)

        # Test using integer as pid
        p = DummyProcessWithOutput.new(pid=5)
        self.assertEquals(p.pid, 5)

        # Test using string as pid
        p = DummyProcessWithOutput.new(pid='a')
        self.assertEquals(p.pid, 'a')

    def test_exception(self):
        proc = ExceptionProcess.new()
        with self.assertRaises(RuntimeError):
            proc.play()
        self.assertEqual(proc.state, ProcessState.FAILED)

    def test_get_description(self):
        # Not all that much we can test for, but check if it's a string at
        # least
        for ProcClass in TEST_PROCESSES:
            desc = ProcClass.get_description()
            self.assertIsInstance(desc, str)

        # Dummy process should at least use the docstring as part of the
        # description and so it shouldn't be empty
        desc = DummyProcess.get_description()
        self.assertNotEqual(desc, "")

    def test_created_bundle(self):
        """
        Check that the bundle after just creating a process is as we expect
        :return:
        """
        proc = DummyProcessWithOutput.new()
        b = Bundle()
        proc.save_instance_state(b)
        self.assertIsNone(b.get('inputs', None))
        self.assertEqual(len(b['outputs']), 0)

    def test_instance_state(self):
        proc = DummyProcessWithOutput.new()

        saver = ProcessSaver(proc)
        proc.play()

        for info, outputs in zip(saver.snapshots, saver.outputs):
            state, bundle = info
            # Check that it is a copy
            self.assertIsNot(
                outputs, bundle[Process.BundleKeys.OUTPUTS.value].get_dict())
            # Check the contents are the same
            self.assertEqual(
                outputs, bundle[Process.BundleKeys.OUTPUTS.value].get_dict())

        self.assertIsNot(
            proc.outputs, saver.snapshots[-1][1][Process.BundleKeys.OUTPUTS.value])

    def test_saving_each_step(self):
        for ProcClass in TEST_PROCESSES:
            proc = ProcClass.new()

            saver = ProcessSaver(proc)
            proc.play()

            self.assertEqual(proc.state, ProcessState.STOPPED)
            self.assertTrue(check_process_against_snapshots(ProcClass, saver.snapshots))

    def test_saving_each_step_interleaved(self):
        for ProcClass in TEST_PROCESSES:
            proc = ProcClass.new()
            ps = ProcessSaver(proc)
            try:
                proc.play()
            except BaseException:
                pass

            self.assertTrue(check_process_against_snapshots(ProcClass, ps.snapshots))

    def test_logging(self):
        class LoggerTester(Process):
            def _run(self, **kwargs):
                self.logger.info("Test")

        # TODO: Test giving a custom logger to see if it gets used
        p = LoggerTester.new()
        p.play()

    def test_abort(self):
        proc = DummyProcess.new()
        proc.abort()
        self.assertTrue(proc.has_aborted())
        self.assertEqual(proc.state, ProcessState.STOPPED)

    def test_wait_continue(self):
        p = WaitForSignalProcess.new()
        self.procman.play(p)
        self.assertTrue(wait_until(p, ProcessState.WAITING, timeout=1.))
        p.continue_()
        self.assertTrue(p.wait(timeout=2.))
        self.assertTrue(p.has_finished())

    def test_wait_pause_continue_play(self):
        p = WaitForSignalProcess.new()

        # Play the process and wait until it is waiting
        self.procman.play(p)

        # Wait
        self.assertTrue(wait_until(p, ProcessState.WAITING, 1.))

        # Pause
        self.assertTrue(p.pause(timeout=1.))

        # Continue
        p.continue_()

        # Play
        p.play()

        self.assertTrue(p.wait(timeout=1.))
        self.assertEqual(p.state, ProcessState.STOPPED)

    def test_wait_pause_play_continue(self):
        p = WaitForSignalProcess.new()

        fut = self.procman.play(p)

        # Wait
        self.assertTrue(wait_until(p, ProcessState.WAITING, 1.))
        self.assertTrue(p.is_playing())

        # Pause
        self.assertTrue(p.pause(timeout=1.))
        self.assertFalse(p.is_playing())

        # Play
        fut.play()
        self.assertEqual(p.state, ProcessState.WAITING)

        # Continue
        p.continue_()
        self.assertTrue(wait_until(p, ProcessState.STOPPED), 1.)

    def test_exc_info(self):
        p = ExceptionProcess.new()
        try:
            p.start()
        except BaseException:
            import sys
            exc_info = sys.exc_info()
            p_exc_info = p.get_exc_info()
            self.assertEqual(p_exc_info[0], exc_info[0])
            self.assertEqual(p_exc_info[1], exc_info[1])

    def test_exception_in_on_playing(self):
        class P(DummyProcess):
            def on_playing(self):
                raise RuntimeError("Cope with this")

        p = P.new()
        with self.assertRaises(RuntimeError):
            p.play()

    def test_exception_in_done_playing(self):
        class P(DummyProcess):
            def on_done_playing(self):
                raise RuntimeError("Cope with this")

        p = P.new()
        with self.assertRaises(RuntimeError):
            p.play()

    def test_direct_instantiate(self):
        with self.assertRaises(AssertionError):
            DummyProcess(inputs={}, pid=None)

    def test_restart(self):
        p = _RestartProcess.new()

        future = self.procman.play(p)
        self.assertTrue(wait_until(p, ProcessState.WAITING, timeout=2.))

        # Save the state of the process
        bundle = Bundle()
        p.save_instance_state(bundle)
        self.assertTrue(future.abort(timeout=2.))

        # Load a process from the saved state
        p = _RestartProcess.load(bundle)
        self.assertEqual(p.state, ProcessState.WAITING)

        # Now play it
        future = self.procman.play(p)
        p.continue_()
        self.assertEqual(future.result(timeout=1.0), {'finished': True})

    def test_wait(self):
        p = DummyProcess.new()
        self.assertTrue(p.wait(timeout=2.), "Not running process didn't return from wait")
        self.procman.play(p)
        self.assertTrue(p.wait(timeout=2.), "Process failed to return from wait when done")

    def test_wait_pause_play(self):
        p = WaitForSignalProcess.new()
        self.procman.play(p)

        # Wait
        self.assertTrue(wait_until(p, ProcessState.WAITING, timeout=2.))

        # Pause
        self.assertTrue(p.pause(timeout=1.))

        # Play
        self.procman.play(p)
        self.assertTrue(wait_until(p, ProcessState.WAITING, timeout=2.))

    def test_pause_play(self):
        """
        Pausing a process that is not playing should have no effect and next 
        time it is played it should continue normally.
        """
        p = DummyProcess.new()
        self.assertTrue(p.pause(timeout=1.))
        p.play()
        self.assertEqual(p.state, ProcessState.STOPPED)

    def _check_process_against_snapshot(self, snapshot, proc):
        self.assertEqual(snapshot.state, proc.state)

        new_bundle = Bundle()
        proc.save_instance_state(new_bundle)
        self.assertEqual(snapshot.bundle, new_bundle,
                         "Bundle mismatch with process class {}\n"
                         "Snapshot:\n{}\n"
                         "Loaded:\n{}".format(
                             proc.__class__, snapshot.bundle, new_bundle))

        self.assertEqual(snapshot.outputs, proc.outputs,
                         "Outputs mismatch with process class {}\n"
                         "Snapshot:\n{}\n"
                         "Loaded:\n{}".format(
                             proc.__class__, snapshot.outputs, proc.outputs))


class TestProcessEvents(TestCase):
    def setUp(self):
        super(TestProcessEvents, self).setUp()

        self.events_tester = ProcessListenerTester()
        self.proc = DummyProcessWithOutput.new()
        self.proc.add_process_listener(self.events_tester)

        self.procman = ProcessManager()

    def tearDown(self):
        self.proc.remove_process_listener(self.events_tester)
        self.procman.abort_all(timeout=10.)
        self.assertEqual(self.procman.get_num_processes(), 0, "Failed to abort all processes")
        super(TestProcessEvents, self).tearDown()

    def test_on_play(self):
        self.proc.play()
        self.assertTrue(self.events_tester.play)

    def test_on_start(self):
        self.proc.play()
        self.assertTrue(self.events_tester.start)

    def test_on_run(self):
        self.proc.play()
        self.assertTrue(self.events_tester.run)

    def test_on_output_emitted(self):
        self.proc.play()
        self.assertTrue(self.events_tester.emitted)

    def test_on_finished(self):
        self.proc.play()
        self.assertTrue(self.events_tester.finish)

    def test_on_done_playing(self):
        self.proc.play()
        self.assertTrue(self.events_tester.done_playing)

    def test_events_run_through(self):
        self.proc.play()
        self.assertTrue(self.events_tester.play)
        self.assertTrue(self.events_tester.start)
        self.assertTrue(self.events_tester.run)
        self.assertTrue(self.events_tester.emitted)
        self.assertTrue(self.events_tester.finish)
        self.assertTrue(self.events_tester.stop)
        self.assertTrue(self.events_tester.done_playing)


class _RestartProcess(WaitForSignalProcess):
    @classmethod
    def define(cls, spec):
        super(_RestartProcess, cls).define(spec)
        spec.dynamic_output()

    def finish(self, wait_on):
        self.out("finished", True)
