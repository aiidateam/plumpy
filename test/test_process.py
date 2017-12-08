import apricotpy
import plum
from plum import Process, ProcessState
from plum.test_utils import DummyProcess, ExceptionProcess, DummyProcessWithOutput, TEST_PROCESSES, ProcessSaver, \
    check_process_against_snapshots, \
    WaitForSignalProcess
from plum.test_utils import ProcessListenerTester
from plum import process
import uuid
from . import util


class ForgetToCallParent(Process):
    @classmethod
    def define(cls, spec):
        super(ForgetToCallParent, cls).define(spec)
        spec.input('forget_on', valid_type=str)

    def _run(self, forget_on):
        pass

    def on_created(self):
        if self.inputs.forget_on != 'created':
            super(ForgetToCallParent, self).on_created()

    def on_running(self):
        if self.inputs.forget_on != 'running':
            super(ForgetToCallParent, self).on_running()

    def on_failed(self, exception):
        if self.inputs.forget_on != 'failed':
            super(ForgetToCallParent, self).on_failed(exception)

    def on_finished(self, result):
        if self.inputs.forget_on != 'finished':
            super(ForgetToCallParent, self).on_finished(result)

    def on_cancelled(self, msg):
        if self.inputs.forget_on != 'cancelled':
            super(ForgetToCallParent, self).on_cancelled(msg)


class TestProcess(util.TestCaseWithLoop):
    def test_spec(self):
        """
        Check that the references to specs are doing the right thing...
        """
        dp = DummyProcess()
        self.assertIsNot(DummyProcess.spec(), Process.spec())
        self.assertIs(dp.spec(), DummyProcess.spec())

        class Proc(DummyProcess):
            pass

        self.assertIsNot(Proc.spec(), Process.spec())
        self.assertIsNot(Proc.spec(), DummyProcess.spec())
        p = Proc()
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
            NoDynamic(inputs={'a': 5}).play()

        proc = WithDynamic(inputs={'a': 5})
        proc.play()
        self.loop.run_until_complete(proc.future())

    def test_inputs(self):
        class Proc(Process):
            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)
                spec.input('a')

            def _run(self, a):
                pass

        p = Proc({'a': 5})

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
        p = Proc(inputs={'input': 2}, loop=self.loop)
        self.assertEqual(p.inputs['input'], 2)

        # Don't supply, use default
        p = Proc()
        self.assertEqual(p.inputs['input'], 5)

    def test_inputs_default_that_evaluate_to_false(self):
        for def_val in (True, False, 0, 1):
            class Proc(DummyProcess):
                @classmethod
                def define(cls, spec):
                    super(Proc, cls).define(spec)
                    spec.input('input', default=def_val)

            # Don't supply, use default
            p = Proc()
            self.assertIn('input', p.inputs)
            self.assertEqual(p.inputs['input'], def_val)

    def test_run(self):
        proc = DummyProcessWithOutput()
        proc.play()
        self.loop.run_until_complete(proc.future())

        self.assertTrue(proc.has_finished())
        self.assertEqual(proc.state, ProcessState.FINISHED)
        self.assertEqual(proc.outputs, {'default': 5})

    def test_run_from_class(self):
        # Test running through class method
        proc = DummyProcessWithOutput()
        proc.execute()
        results = proc.outputs
        self.assertEqual(results['default'], 5)

    def test_forget_to_call_parent(self):
        for event in ('created', 'running', 'finished'):
            with self.assertRaises(plum.TransitionFailed):
                proc = ForgetToCallParent(inputs={'forget_on': event})
                proc.play()
                self.loop.run_until_complete(proc.future())

    def test_pid(self):
        # Test auto generation of pid
        p = DummyProcessWithOutput()
        self.assertIsNotNone(p.pid)

        # Test using integer as pid
        p = DummyProcessWithOutput(pid=5)
        self.assertEquals(p.pid, 5)

        # Test using string as pid
        p = DummyProcessWithOutput(pid='a')
        self.assertEquals(p.pid, 'a')

    def test_exception(self):
        proc = ExceptionProcess()
        proc.play()
        with self.assertRaises(RuntimeError):
            self.loop.run_until_complete(proc.future())
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
        proc = DummyProcessWithOutput()
        b = plum.Bundle(proc)

        self.assertIsNone(b.get(process.BundleKeys.INPUTS, None))
        self.assertEqual(len(b[process.BundleKeys.OUTPUTS]), 0)

    def test_instance_state(self):
        BundleKeys = process.BundleKeys
        proc = DummyProcessWithOutput()

        saver = ProcessSaver(proc)
        proc.play()
        proc.execute()

        for bundle, outputs in zip(saver.snapshots, saver.outputs):
            # Check that it is a copy
            self.assertIsNot(outputs, bundle[BundleKeys.OUTPUTS])
            # Check the contents are the same
            self.assertEqual(outputs, bundle[BundleKeys.OUTPUTS])

        self.assertIsNot(
            proc.outputs, saver.snapshots[-1][BundleKeys.OUTPUTS]
        )

    def test_saving_each_step(self):
        for proc_class in TEST_PROCESSES:
            proc = proc_class()
            saver = ProcessSaver(proc)
            proc.execute()

            self.assertEqual(proc.state, ProcessState.FINISHED)
            self.assertTrue(
                check_process_against_snapshots(self.loop, proc_class, saver.snapshots)
            )

    def test_saving_each_step_interleaved(self):
        for ProcClass in TEST_PROCESSES:
            proc = ProcClass()
            proc.play()
            ps = ProcessSaver(proc)
            try:
                self.loop.run_until_complete(util.HansKlok(proc))
            except BaseException:
                pass

            self.assertTrue(
                check_process_against_snapshots(self.loop, ProcClass, ps.snapshots)
            )

    def test_logging(self):
        class LoggerTester(Process):
            def _run(self, **kwargs):
                self.logger.info("Test")

        # TODO: Test giving a custom logger to see if it gets used
        proc = LoggerTester()
        proc.play()
        self.loop.run_until_complete(proc.future())

    def test_cancel(self):
        proc = DummyProcess(loop=self.loop)

        proc.cancel('Farewell!')
        self.assertTrue(proc.cancelled())
        self.assertEqual(proc.cancelled_msg(), 'Farewell!')
        self.assertEqual(proc.state, ProcessState.CANCELLED)

    def test_wait_continue(self):
        proc = WaitForSignalProcess()
        # Wait - Execute the process and wait until it is waiting
        proc.execute(True)
        proc.resume()
        proc.execute(True)

        # Check it's done
        self.assertEqual(proc.state, ProcessState.FINISHED)
        self.assertTrue(proc.has_finished())

    def test_exc_info(self):
        proc = ExceptionProcess()
        try:
            proc.execute()
        except RuntimeError as e:
            self.assertEqual(proc.exception(), e)

    def test_restart(self):
        proc = _RestartProcess()
        proc.execute(True)

        # Save the state of the process
        saved_state = plum.Bundle(proc)

        # Load a process from the saved state
        proc = saved_state.unbundle()
        self.assertEqual(proc.state, ProcessState.WAITING)

        # Now play it
        proc.resume()
        result = proc.execute(True)
        self.assertEqual(proc.outputs, {'finished': True})

    def test_run_done(self):
        proc = DummyProcess()
        proc.play()
        self.loop.run_until_complete(proc.future())
        self.assertTrue(proc.done())

    def test_wait_pause_play_resume(self):
        """
        Test that if you pause a process that and its awaitable finishes that it
        completes correctly when played again.
        """
        proc = WaitForSignalProcess()

        # Wait - Run the process and wait until it is waiting
        proc.execute(True)

        proc.pause()
        self.assertEqual(proc.state, ProcessState.PAUSED)
        proc.play()
        self.assertEqual(proc.state, ProcessState.WAITING)
        proc.resume()

        # Run
        proc.execute(True)

        # Check it's done
        self.assertEqual(proc.state, ProcessState.FINISHED)
        self.assertTrue(proc.has_finished())

    def test_wait_save_continue(self):
        """ Test that process saved while in WAITING state restarts correctly when loaded """
        proc = WaitForSignalProcess()
        proc.play()

        # Wait - Run the process until it enters the WAITING state
        proc.execute(True)

        saved_state = plum.Bundle(proc)

        # Run the process to the end
        proc.resume()
        result = proc.execute()

        # Load from saved state and run again
        proc = saved_state.unbundle(self.loop)
        proc.resume()
        result2 = proc.execute()

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


class TestProcessEvents(util.TestCaseWithLoop):
    def setUp(self):
        super(TestProcessEvents, self).setUp()
        self.events_tester = ProcessListenerTester()
        self.proc = DummyProcessWithOutput()
        self.proc.add_process_listener(self.events_tester)
        self.proc.play()

    def tearDown(self):
        self.proc.remove_process_listener(self.events_tester)
        super(TestProcessEvents, self).tearDown()

    def test_basic_events(self):
        self.loop.run_until_complete(self.proc.future())
        for evt in ['running', 'output_emitted', 'finished']:
            self.assertIn(evt, self.events_tester.called)

    def test_cancelled(self):
        self.proc.cancel()
        self.assertTrue(self.proc.cancelled())
        self.assertIn('cancelled', self.events_tester.called)

    def test_failed(self):
        self.proc.fail(RuntimeError('See ya later suckers'))
        self.assertIsNotNone(self.proc.exception())
        self.assertIn('failed', self.events_tester.called)

    def test_paused(self):
        self.proc.pause()
        self.assertIn('paused', self.events_tester.called)


class _RestartProcess(WaitForSignalProcess):
    @classmethod
    def define(cls, spec):
        super(_RestartProcess, cls).define(spec)
        spec.dynamic_output()

    def last_step(self):
        self.out("finished", True)
