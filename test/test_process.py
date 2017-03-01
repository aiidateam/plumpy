import threading
from plum.persistence.bundle import Bundle
from plum.process import Process, ProcessState
from plum.process_monitor import MONITOR
from plum.test_utils import DummyProcess, ExceptionProcess, TwoCheckpoint, \
    DummyProcessWithOutput, TEST_PROCESSES, ProcessSaver, check_process_against_snapshots, \
    WaitForSignalProcess
from plum.test_utils import ProcessListenerTester
from plum.util import override
from plum.wait_ons import wait_until
from util import TestCase


class ForgetToCallParent(Process):
    @override
    def _run(self):
        pass

    @override
    def on_start(self):
        pass

    @override
    def on_run(self):
        pass

    @override
    def on_fail(self):
        pass

    @override
    def on_finish(self):
        pass

    @override
    def on_stop(self):
        pass


class TestProcess(TestCase):
    def setUp(self):
        super(TestProcess, self).setUp()

        self.events_tester = ProcessListenerTester()
        self.proc = DummyProcessWithOutput.new()
        self.proc.add_process_listener(self.events_tester)

    def tearDown(self):
        super(TestProcess, self).tearDown()

        self.proc.remove_process_listener(self.events_tester)

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

    def test_on_run(self):
        self.proc.on_run()
        self.assertTrue(self.events_tester.run)

    def test_on_output_emitted(self):
        self.proc._run()
        self.assertTrue(self.events_tester.emitted)

    def test_on_finished(self):
        self.proc.on_finish()
        self.assertTrue(self.events_tester.finish)

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
        p._perform_create()
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
        p = ForgetToCallParent.new()

        with self.assertRaises(AssertionError):
            p._perform_start()

        with self.assertRaises(AssertionError):
            p._perform_finish()

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
        proc.play()
        self.assertIsInstance(proc.get_exception(), BaseException)
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
        # Abort a process before it gets started, this will get ignored and the
        # process will run normally
        proc = DummyProcess.new()
        try:
            proc.abort()
        except AssertionError:
            pass
        proc.play()

        self.assertFalse(proc.has_aborted())
        self.assertEqual(proc.state, ProcessState.STOPPED)

    def test_wait_continue(self):
        p = WaitForSignalProcess.new()
        t = threading.Thread(target=p.play)
        t.start()
        self.assertTrue(wait_until(p, ProcessState.WAITING, 1.))
        self.assertEqual(p.state, ProcessState.WAITING)

        self.assertTrue(p.is_playing())
        p.continue_()

        self.assertTrue(wait_until(p, ProcessState.STOPPED, 1.))
        self.assertEqual(p.state, ProcessState.STOPPED)

    def test_wait_pause_continue_play(self):
        p = WaitForSignalProcess.new()
        t = threading.Thread(target=p.play)
        t.start()
        self.assertTrue(wait_until(p, ProcessState.WAITING, 5))

        self.assertTrue(p.is_playing())
        p.pause()
        self.safe_join(t)
        self.assertFalse(p.is_playing())

        p.continue_()
        p.play()

        self.assertEqual(p.state, ProcessState.STOPPED)

    def test_wait_pause_play_continue(self):
        p = WaitForSignalProcess.new()

        t = threading.Thread(target=p.play)
        t.start()
        self.assertTrue(wait_until(p, ProcessState.WAITING, 5))

        self.assertTrue(p.is_playing())
        p.pause()
        self.safe_join(t)
        self.assertFalse(p.is_playing())

        t = threading.Thread(target=p.play)
        t.start()
        self.assertEqual(p.state, ProcessState.WAITING)
        p.continue_()

        self.assertTrue(wait_until(p, ProcessState.STOPPED), 5)
        self.safe_join(t, 5)
        self.assertFalse(t.is_alive())

    def test_exception_in_on_playing(self):
        class P(DummyProcess):
            def on_playing(self):
                raise RuntimeError("Cope with this")

        p = P.new()
        p.play()
        self.assertIsInstance(p.get_exception(), RuntimeError)

    def test_exception_in_done_playing(self):
        class P(DummyProcess):
            def on_done_playing(self):
                raise RuntimeError("Cope with this")

        p = P.new()
        p.play()
        self.assertIsInstance(p.get_exception(), RuntimeError)

    def test_direct_instantiate(self):
        with self.assertRaises(AssertionError):
            DummyProcess(inputs={}, pid=None)

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
