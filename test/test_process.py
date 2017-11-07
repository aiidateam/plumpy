import apricotpy
import plum
from plum import Process, ProcessState
from plum.test_utils import DummyProcess, ExceptionProcess, DummyProcessWithOutput, TEST_PROCESSES, ProcessSaver, \
    check_process_against_snapshots, \
    WaitForSignalProcess
from plum.test_utils import ProcessListenerTester
from plum.utils import override
from plum.wait_ons import run_until, WaitOnProcessState
from plum import process
import uuid
from . import util


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
            self.loop.run_until_complete(util.HansKlok(NoDynamic(inputs={'a': 5}).play()))

        self.loop.run_until_complete(util.HansKlok(WithDynamic(inputs={'a': 5}).play()))

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
        proc = DummyProcessWithOutput().play()
        self.loop.run_until_complete(util.HansKlok(proc))

        self.assertTrue(proc.has_finished())
        self.assertEqual(proc.state, ProcessState.STOPPED)
        self.assertEqual(proc.outputs, {'default': 5})

    def test_run_from_class(self):
        # Test running through class method
        results = self.loop.run_until_complete(DummyProcessWithOutput().play())
        self.assertEqual(results['default'], 5)

    def test_forget_to_call_parent(self):
        for event in ('create', 'start', 'run', 'finish', 'stop'):
            with self.assertRaises(AssertionError):
                self.loop.run_until_complete(
                    util.HansKlok(ForgetToCallParent(inputs={'forget_on': event}).play())
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
        proc = ExceptionProcess().play()
        with self.assertRaises(RuntimeError):
            self.loop.run_until_complete(util.HansKlok(proc))
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
        proc = self.loop.create(DummyProcessWithOutput)
        b = apricotpy.persistable.Bundle(proc)

        self.assertIsNone(b.get(process.BundleKeys.INPUTS, None))
        self.assertEqual(len(b[process.BundleKeys.OUTPUTS]), 0)

    def test_instance_state(self):
        BundleKeys = process.BundleKeys
        proc = DummyProcessWithOutput().play()

        saver = ProcessSaver(proc)
        self.loop.run_until_complete(util.HansKlok(proc))

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
            proc = proc_class().play()

            saver = ProcessSaver(proc)
            self.loop.run_until_complete(util.HansKlok(proc))

            self.assertEqual(proc.state, ProcessState.STOPPED)
            self.assertTrue(
                check_process_against_snapshots(self.loop, proc_class, saver.snapshots)
            )

    def test_saving_each_step_interleaved(self):
        for ProcClass in TEST_PROCESSES:
            proc = ProcClass().play()
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
        self.loop.run_until_complete(util.HansKlok(LoggerTester().play()))

    def test_abort(self):
        proc = self.loop.create(DummyProcess)

        aborted = ~proc.abort('Farewell!')
        self.assertTrue(aborted)
        self.assertTrue(proc.has_aborted())
        self.assertEqual(proc.get_abort_msg(), 'Farewell!')
        self.assertEqual(proc.state, ProcessState.STOPPED)

    def test_wait_continue(self):
        proc = WaitForSignalProcess().play()

        # Wait - Run the process and wait until it is waiting
        run_until(proc, ProcessState.WAITING, self.loop)

        proc.continue_()

        # Run
        self.loop.run_until_complete(util.HansKlok(proc))

        # Check it's done
        self.assertEqual(proc.state, ProcessState.STOPPED)
        self.assertTrue(proc.has_finished())

    def test_exc_info(self):
        proc = ExceptionProcess().play()
        try:
            self.loop.run_until_complete(util.HansKlok( proc))
        except RuntimeError as e:
            self.assertEqual(proc.exception(), e)

    def test_restart(self):
        proc = _RestartProcess().play()
        run_until(proc, ProcessState.WAITING, self.loop)

        # Save the state of the process
        saved_state = apricotpy.persistable.Bundle(proc)
        ~proc.abort()

        # Load a process from the saved state
        proc = saved_state.unbundle(self.loop)
        self.assertEqual(proc.state, ProcessState.WAITING)

        # Now play it
        proc.continue_()
        result = ~proc
        self.assertEqual(result, {'finished': True})

    def test_run_terminated(self):
        proc = DummyProcess().play()
        self.loop.run_until_complete(proc)
        self.assertTrue(proc.has_terminated())

    def test_wait_pause_continue_play(self):
        """
        Test that if you pause a process that and its awaitable finishes that it
        completes correctly when played again.
        """
        proc = WaitForSignalProcess().play()

        # Wait - Run the process and wait until it is waiting
        run_until(proc, ProcessState.WAITING, self.loop)

        proc.pause()
        proc.continue_()
        proc.play()

        # Run
        self.loop.run_until_complete(util.HansKlok(proc))

        # Check it's done
        self.assertEqual(proc.state, ProcessState.STOPPED)
        self.assertTrue(proc.has_finished())

    def test_wait_pause_continue_tick_play(self):
        """
        Test that if you pause a process that and its awaitable finishes that it
        completes correctly when played again.
        """
        proc = WaitForSignalProcess().play()

        # Wait - Run the process and wait until it is waiting
        run_until(proc, ProcessState.WAITING, self.loop)

        proc.pause()
        proc.continue_()
        self.loop.tick()  # This should schedule the awaitable done callback
        self.loop.tick()  # Now the callback should be processed
        proc.play()

        # Run
        self.loop.run_until_complete(util.HansKlok(proc))

        # Check it's done
        self.assertEqual(proc.state, ProcessState.STOPPED)
        self.assertTrue(proc.has_finished())

    def test_wait_save_continue(self):
        """ Test that process saved while in WAITING state restarts correctly when loaded """
        proc = WaitForSignalProcess().play()

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

    def test_status_request(self):
        """ Test that a status request is acknowledged by a process. """
        results = []
        _got_status = util.get_message_capture_fn(results)

        my_id = uuid.uuid4()
        self.loop.messages().add_listener(_got_status, subject_filter=plum.ProcessMessage.STATUS_REPORT)
        proc = DummyProcess().play()
        self.loop.messages().send(
            sender_id=my_id,
            to=proc.uuid,
            subject=plum.ProcessAction.REPORT_STATUS
            )
        self.loop.run_until_complete(util.HansKlok(proc))

        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result['subject'], plum.ProcessMessage.STATUS_REPORT)
        self.assertIn('body', result)

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
        self.proc = DummyProcessWithOutput().play()
        self.proc.add_process_listener(self.events_tester)

    def tearDown(self):
        self.proc.remove_process_listener(self.events_tester)
        super(TestProcessEvents, self).tearDown()

    def test_on_start(self):
        self.loop.run_until_complete(util.HansKlok(self.proc))
        self.assertTrue(self.events_tester.start)

    def test_on_run(self):
        self.loop.run_until_complete(util.HansKlok(self.proc))
        self.assertTrue(self.events_tester.run)

    def test_on_output_emitted(self):
        self.loop.run_until_complete(util.HansKlok(self.proc))
        self.assertTrue(self.events_tester.emitted)

    def test_on_finished(self):
        self.loop.run_until_complete(util.HansKlok(self.proc))
        self.assertTrue(self.events_tester.finish)

    def test_events_run_through(self):
        self.loop.run_until_complete(util.HansKlok(self.proc))
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
