import apricotpy
import plum
from plum import loop_factory
from plum import Process, ProcessState
from plum.test_utils import DummyProcess, ExceptionProcess, DummyProcessWithOutput, TEST_PROCESSES, ProcessSaver, \
    check_process_against_snapshots, \
    WaitForSignalProcess
from plum.test_utils import ProcessListenerTester
from plum.utils import override
from plum.wait_ons import run_until, WaitOnProcessState
from .util import TestCase


class ForgetToCallParent(Process):
    @classmethod
    def define(cls, spec):
        super(ForgetToCallParent, cls).define(spec)
        spec.input('forget_on', valid_type=str)

    @override
    def _run(self, forget_on):
        pass

    @override
    def on_create(self):
        if self.inputs.forget_on != 'create':
            super(ForgetToCallParent, self).on_create()

    @override
    def on_start(self):
        if self.inputs.forget_on != 'start':
            super(ForgetToCallParent, self).on_start()

    @override
    def on_run(self):
        if self.inputs.forget_on != 'run':
            super(ForgetToCallParent, self).on_start()

    @override
    def on_fail(self, exc_info):
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
        self.loop = loop_factory()

    def test_spec(self):
        """
        Check that the references to specs are doing the right thing...
        """
        dp = self.loop.create(DummyProcess)
        self.assertIsNot(DummyProcess.spec(), Process.spec())
        self.assertIs(dp.spec(), DummyProcess.spec())

        class Proc(DummyProcess):
            pass

        self.assertIsNot(Proc.spec(), Process.spec())
        self.assertIsNot(Proc.spec(), DummyProcess.spec())
        p = self.loop.create(Proc)
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
            self.loop.run_until_complete(self.loop.create(NoDynamic, {'a': 5}))

        self.loop.run_until_complete(self.loop.create(WithDynamic, {'a': 5}))

    def test_inputs(self):
        class Proc(Process):
            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)
                spec.input('a')

            def _run(self, a):
                pass

        p = self.loop.create(Proc, {'a': 5})

        # Check that we can access the inputs after creating
        self.assertEqual(p.raw_inputs.a, 5)
        with self.assertRaises(AttributeError):
            p.raw_inputs.b

    def test_inputs_default(self):
        class Proc(DummyProcess):
            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)
                spec.input('input', default=5, required=False)

        # Supply a value
        p = self.loop.create(Proc, inputs={'input': 2})
        self.assertEqual(p.inputs['input'], 2)

        # Don't supply, use default
        p = self.loop.create(Proc)
        self.assertEqual(p.inputs['input'], 5)

    def test_inputs_default_that_evaluate_to_false(self):
        for def_val in (True, False, 0, 1):
            class Proc(DummyProcess):
                @classmethod
                def define(cls, spec):
                    super(Proc, cls).define(spec)
                    spec.input('input', default=def_val)

            # Don't supply, use default
            p = self.loop.create(Proc)
            self.assertIn('input', p.inputs)
            self.assertEqual(p.inputs['input'], def_val)

    def test_run(self):
        p = self.loop.create(DummyProcessWithOutput)
        self.loop.run_until_complete(p)

        self.assertTrue(p.has_finished())
        self.assertEqual(p.state, ProcessState.STOPPED)
        self.assertEqual(p.outputs, {'default': 5})

    def test_run_from_class(self):
        # Test running through class method

        results = self.loop.run_until_complete(
            self.loop.create(DummyProcessWithOutput)
        )
        self.assertEqual(results['default'], 5)

    def test_forget_to_call_parent(self):
        for event in ('create', 'start', 'run', 'finish', 'stop'):
            with self.assertRaises(AssertionError):
                self.loop.run_until_complete(
                    self.loop.create(ForgetToCallParent, {'forget_on': event})
                )

    def test_pid(self):
        # Test auto generation of pid
        p = self.loop.create(DummyProcessWithOutput)
        self.assertIsNotNone(p.pid)

        # Test using integer as pid
        p = self.loop.create(DummyProcessWithOutput, pid=5)
        self.assertEquals(p.pid, 5)

        # Test using string as pid
        p = self.loop.create(DummyProcessWithOutput, pid='a')
        self.assertEquals(p.pid, 'a')

    def test_exception(self):
        proc = self.loop.create(ExceptionProcess)
        with self.assertRaises(RuntimeError):
            self.loop.run_until_complete(proc)
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
        proc = ~self.loop.create_inserted(DummyProcessWithOutput)
        b = apricotpy.persistable.Bundle(proc)

        self.assertIsNone(b.get(plum.process.BundleKeys.INPUTS, None))
        self.assertEqual(len(b[plum.process.BundleKeys.OUTPUTS]), 0)

    def test_instance_state(self):
        BundleKeys = plum.process.BundleKeys

        proc = self.loop.create(DummyProcessWithOutput)

        saver = ProcessSaver(proc)
        self.loop.run_until_complete(proc)

        for info, outputs in zip(saver.snapshots, saver.outputs):
            state, bundle = info
            # Check that it is a copy
            self.assertIsNot(outputs, bundle[BundleKeys.OUTPUTS])
            # Check the contents are the same
            self.assertEqual(outputs, bundle[BundleKeys.OUTPUTS])

        self.assertIsNot(
            proc.outputs, saver.snapshots[-1][1][BundleKeys.OUTPUTS])

    def test_saving_each_step(self):
        for ProcClass in TEST_PROCESSES:
            proc = self.loop.create(ProcClass)

            saver = ProcessSaver(proc)
            self.loop.run_until_complete(proc)

            self.assertEqual(proc.state, ProcessState.STOPPED)
            self.assertTrue(check_process_against_snapshots(self.loop, ProcClass, saver.snapshots))

    def test_saving_each_step_interleaved(self):
        for ProcClass in TEST_PROCESSES:
            proc = self.loop.create(ProcClass)
            ps = ProcessSaver(proc)
            try:
                self.loop.run_until_complete(proc)
            except BaseException:
                pass

            self.assertTrue(check_process_against_snapshots(self.loop, ProcClass, ps.snapshots))

    def test_logging(self):
        class LoggerTester(Process):
            def _run(self, **kwargs):
                self.logger.info("Test")

        # TODO: Test giving a custom logger to see if it gets used
        self.loop.run_until_complete(self.loop.create(LoggerTester))

    def test_abort(self):
        proc = ~self.loop.create_inserted(DummyProcess)

        aborted = ~proc.abort('Farewell!')
        self.assertTrue(aborted)
        self.assertTrue(proc.has_aborted())
        self.assertEqual(proc.get_abort_msg(), 'Farewell!')
        self.assertEqual(proc.state, ProcessState.STOPPED)

    def test_wait_continue(self):
        proc = self.loop.create(WaitForSignalProcess)

        # Wait - Run the process and wait until it is waiting
        run_until(proc, ProcessState.WAITING, self.loop)

        proc.continue_()

        # Run
        self.loop.run_until_complete(proc)

        # Check it's done
        self.assertEqual(proc.state, ProcessState.STOPPED)
        self.assertTrue(proc.has_finished())

    def test_exc_info(self):
        p = self.loop.create(ExceptionProcess)
        try:
            self.loop.run_until_complete(p)
        except RuntimeError as e:
            self.assertEqual(p.exception(), e)

    def test_restart(self):
        process = self.loop.create(_RestartProcess)
        run_until(process, ProcessState.WAITING, self.loop)

        # Save the state of the process
        saved_state = apricotpy.persistable.Bundle(process)
        ~self.loop.remove(process)

        # Load a process from the saved state
        process = saved_state.unbundle(self.loop)
        self.assertEqual(process.state, ProcessState.WAITING)

        # Now play it
        process.continue_()
        result = ~process
        self.assertEqual(result, {'finished': True})

    def test_run_terminated(self):
        p = self.loop.create(DummyProcess)
        self.loop.run_until_complete(p)
        self.assertTrue(p.has_terminated())

    def test_wait_pause_continue_play(self):
        """
        Test that if you pause a process that and its awaitable finishes that it
        completes correctly when played again.
        """
        proc = self.loop.create(WaitForSignalProcess)

        # Wait - Run the process and wait until it is waiting
        run_until(proc, ProcessState.WAITING, self.loop)

        proc.pause()
        proc.continue_()
        proc.play()

        # Run
        result = ~proc

        # Check it's done
        self.assertEqual(proc.state, ProcessState.STOPPED)
        self.assertTrue(proc.has_finished())

    def test_wait_pause_continue_tick_play(self):
        """
        Test that if you pause a process that and its awaitable finishes that it
        completes correctly when played again.
        """
        proc = self.loop.create(WaitForSignalProcess)

        # Wait - Run the process and wait until it is waiting
        run_until(proc, ProcessState.WAITING, self.loop)

        proc.pause()
        proc.continue_()
        self.loop.tick()  # This should schedule the awaitable done callback
        self.loop.tick()  # Now the callback should be processed
        proc.play()

        # Run
        result = ~proc

        # Check it's done
        self.assertEqual(proc.state, ProcessState.STOPPED)
        self.assertTrue(proc.has_finished())

    def test_wait_save_continue(self):
        """ Test that process saved while in WAITING state restarts correctly when loaded """
        proc = self.loop.create(WaitForSignalProcess)

        # Wait - Run the process until it enters the WAITING state
        run_until(proc, ProcessState.WAITING, self.loop)

        saved_state = apricotpy.persistable.Bundle(proc)

        # Run the process to the end
        proc.continue_()
        result = ~proc

        # Load from saved state and run again
        proc = saved_state.unbundle(self.loop)
        proc.continue_()
        result2 = ~proc

        # Check results match
        self.assertEqual(result, result2)

    def _check_process_against_snapshot(self, snapshot, proc):
        self.assertEqual(snapshot.state, proc.state)

        new_bundle = apricotpy.Bundle()
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

        self.loop = loop_factory()

        self.events_tester = ProcessListenerTester()
        self.proc = self.loop.create(DummyProcessWithOutput)
        self.proc.add_process_listener(self.events_tester)

    def tearDown(self):
        self.proc.remove_process_listener(self.events_tester)
        super(TestProcessEvents, self).tearDown()

    def test_on_start(self):
        self.loop.run_until_complete(self.proc)
        self.assertTrue(self.events_tester.start)

    def test_on_run(self):
        self.loop.run_until_complete(self.proc)
        self.assertTrue(self.events_tester.run)

    def test_on_output_emitted(self):
        self.loop.run_until_complete(self.proc)
        self.assertTrue(self.events_tester.emitted)

    def test_on_finished(self):
        self.loop.run_until_complete(self.proc)
        self.assertTrue(self.events_tester.finish)

    def test_events_run_through(self):
        self.loop.run_until_complete(self.proc)
        self.assertTrue(self.events_tester.start)
        self.assertTrue(self.events_tester.run)
        self.assertTrue(self.events_tester.emitted)
        self.assertTrue(self.events_tester.finish)
        self.assertTrue(self.events_tester.stop)
        self.assertTrue(self.events_tester.terminate)


class _RestartProcess(WaitForSignalProcess):
    @classmethod
    def define(cls, spec):
        super(_RestartProcess, cls).define(spec)
        spec.dynamic_output()

    def finish(self):
        self.out("finished", True)
