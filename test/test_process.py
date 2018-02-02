import plum
import kiwipy
import unittest
from past.builtins import basestring
from plum import Process, ProcessState
from plum import test_utils
from plum import process
from plum.utils import AttributesFrozendict

from . import utils


class ForgetToCallParent(Process):
    def __init__(self, forget_on):
        super(ForgetToCallParent, self).__init__()
        self.forget_on = forget_on

    def _run(self):
        pass

    def on_create(self):
        if self.forget_on != 'create':
            super(ForgetToCallParent, self).on_create()

    def on_run(self):
        if self.forget_on != 'run':
            super(ForgetToCallParent, self).on_run()

    def on_fail(self, exception):
        if self.forget_on != 'fail':
            super(ForgetToCallParent, self).on_fail(exception)

    def on_finish(self, result):
        if self.forget_on != 'finish':
            super(ForgetToCallParent, self).on_finish(result)

    def on_cancel(self, msg):
        if self.forget_on != 'cancel':
            super(ForgetToCallParent, self).on_cancel(msg)


class TestProcess(utils.TestCaseWithLoop):
    def test_spec(self):
        """
        Check that the references to specs are doing the right thing...
        """
        dp = test_utils.DummyProcess()
        self.assertIsNot(test_utils.DummyProcess.spec(), Process.spec())
        self.assertIs(dp.spec(), test_utils.DummyProcess.spec())

        class Proc(test_utils.DummyProcess):
            pass

        self.assertIsNot(Proc.spec(), Process.spec())
        self.assertIsNot(Proc.spec(), test_utils.DummyProcess.spec())
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
                spec.inputs.dynamic = True

            def _run(self, **kwargs):
                pass

        with self.assertRaises(ValueError):
            NoDynamic(inputs={'a': 5}).play()

        proc = WithDynamic(inputs={'a': 5})
        proc.execute()

    def test_inputs(self):
        class Proc(Process):
            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)
                spec.input('a')

            def _run(self):
                pass

        p = Proc({'a': 5})

        # Check that we can access the inputs after creating
        self.assertEqual(p.raw_inputs.a, 5)
        with self.assertRaises(AttributeError):
            p.raw_inputs.b

    def test_inputs_default(self):
        class Proc(test_utils.DummyProcess):
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
            class Proc(test_utils.DummyProcess):
                @classmethod
                def define(cls, spec):
                    super(Proc, cls).define(spec)
                    spec.input('input', default=def_val)

            # Don't supply, use default
            p = Proc()
            self.assertIn('input', p.inputs)
            self.assertEqual(p.inputs['input'], def_val)

    def test_execute(self):
        proc = test_utils.DummyProcessWithOutput()
        proc.execute()

        self.assertTrue(proc.done())
        self.assertEqual(proc.state, ProcessState.FINISHED)
        self.assertEqual(proc.outputs, {'default': 5})

    def test_run_from_class(self):
        # Test running through class method
        proc = test_utils.DummyProcessWithOutput()
        proc.execute()
        results = proc.outputs
        self.assertEqual(results['default'], 5)

    def test_forget_to_call_parent(self):
        for event in ('create', 'run', 'finish'):
            with self.assertRaises(AssertionError):
                proc = ForgetToCallParent(event)
                proc.execute()

    def test_forget_to_call_parent_cancel(self):
        with self.assertRaises(AssertionError):
            proc = ForgetToCallParent('cancel')
            proc.cancel()
            proc.execute()

    def test_pid(self):
        # Test auto generation of pid
        process = test_utils.DummyProcessWithOutput()
        self.assertIsNotNone(process.pid)

        # Test using integer as pid
        process = test_utils.DummyProcessWithOutput(pid=5)
        self.assertEquals(process.pid, 5)

        # Test using string as pid
        process = test_utils.DummyProcessWithOutput(pid='a')
        self.assertEquals(process.pid, 'a')

    def test_exception(self):
        proc = test_utils.ExceptionProcess()
        proc.play()
        with self.assertRaises(RuntimeError):
            proc.execute()
        self.assertEqual(proc.state, ProcessState.FAILED)

    def test_get_description(self):
        class ProcWithoutSpec(Process):
            pass

        class ProcWithSpec(Process):
            """ Process with a spec and a docstring """

            @classmethod
            def define(cls, spec):
                super(ProcWithSpec, cls).define(spec)
                spec.input('a', default=1)

        for proc_class in test_utils.TEST_PROCESSES:
            desc = proc_class.get_description()
            self.assertIsInstance(desc, dict)


        desc_with_spec = ProcWithSpec.get_description()
        desc_without_spec = ProcWithoutSpec.get_description()

        self.assertIsInstance(desc_without_spec, dict)
        self.assertTrue('spec' in desc_without_spec)
        self.assertTrue('description' not in desc_without_spec)
        self.assertIsInstance(desc_with_spec['spec'], dict)

        self.assertIsInstance(desc_with_spec, dict)
        self.assertTrue('spec' in desc_with_spec)
        self.assertTrue('description' in desc_with_spec)
        self.assertIsInstance(desc_with_spec['spec'], dict)
        self.assertIsInstance(desc_with_spec['description'], basestring)

    def test_created_bundle(self):
        """
        Check that the bundle after just creating a process is as we expect
        :return:
        """
        proc = test_utils.DummyProcessWithOutput()
        b = plum.Bundle(proc)

        self.assertIsNone(b.get(process.BundleKeys.INPUTS, None))
        self.assertEqual(len(b[process.BundleKeys.OUTPUTS]), 0)

    def test_instance_state(self):
        proc = test_utils.DummyProcessWithOutput()

        saver = test_utils.ProcessSaver(proc)
        proc.play()
        proc.execute()

        for bundle, outputs in zip(saver.snapshots, saver.outputs):
            # Check that it is a copy
            self.assertIsNot(outputs, bundle[process.BundleKeys.OUTPUTS])
            # Check the contents are the same
            self.assertEqual(outputs, bundle[process.BundleKeys.OUTPUTS])

        self.assertIsNot(
            proc.outputs, saver.snapshots[-1][process.BundleKeys.OUTPUTS]
        )

    def test_saving_each_step(self):
        for proc_class in test_utils.TEST_PROCESSES:
            proc = proc_class()
            saver = test_utils.ProcessSaver(proc)
            saver.capture()
            self.assertEqual(proc.state, ProcessState.FINISHED)
            self.assertTrue(
                test_utils.check_process_against_snapshots(
                    self.loop, proc_class, saver.snapshots)
            )

    def test_saving_each_step_interleaved(self):
        for ProcClass in test_utils.TEST_PROCESSES:
            proc = ProcClass()
            saver = test_utils.ProcessSaver(proc)
            saver.capture()

            self.assertTrue(
                test_utils.check_process_against_snapshots(
                    self.loop, ProcClass, saver.snapshots)
            )

    def test_logging(self):
        class LoggerTester(Process):
            def _run(self, **kwargs):
                self.logger.info("Test")

        # TODO: Test giving a custom logger to see if it gets used
        proc = LoggerTester()
        proc.execute()

    def test_cancel(self):
        proc = test_utils.DummyProcess(loop=self.loop)

        proc.cancel('Farewell!')
        self.assertTrue(proc.cancelled())
        self.assertEqual(proc.cancelled_msg(), 'Farewell!')
        self.assertEqual(proc.state, ProcessState.CANCELLED)

    def test_wait_continue(self):
        proc = test_utils.WaitForSignalProcess()
        # Wait - Execute the process and wait until it is waiting
        proc.execute(True)
        proc.resume()
        proc.execute(True)

        # Check it's done
        self.assertTrue(proc.done())
        self.assertEqual(proc.state, ProcessState.FINISHED)

    def test_exc_info(self):
        proc = test_utils.ExceptionProcess()
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
        proc = test_utils.DummyProcess()
        proc.execute()
        self.assertTrue(proc.done())

    def test_wait_pause_play_resume(self):
        """
        Test that if you pause a process that and its awaitable finishes that it
        completes correctly when played again.
        """
        proc = test_utils.WaitForSignalProcess()

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
        self.assertTrue(proc.done())
        self.assertEqual(proc.state, ProcessState.FINISHED)

    def test_wait_save_continue(self):
        """ Test that process saved while in WAITING state restarts correctly when loaded """
        proc = test_utils.WaitForSignalProcess()
        proc.play()

        # Wait - Run the process until it enters the WAITING state
        proc.execute(True)

        saved_state = plum.Bundle(proc)

        # Run the process to the end
        proc.resume()
        result = proc.execute()

        # Load from saved state and run again
        proc = saved_state.unbundle(loop=self.loop)
        proc.resume()
        result2 = proc.execute()

        # Check results match
        self.assertEqual(result, result2)

    def test_cancel_in_run(self):
        class CancelProcess(Process):
            after_cancel = False

            def _run(self, **kwargs):
                self.cancel()
                self.after_cancel = True

        proc = CancelProcess()
        with self.assertRaises(plum.CancelledError):
            proc.execute()

        self.assertFalse(proc.after_cancel)
        self.assertEqual(proc.state, ProcessState.CANCELLED)

    def test_run_multiple(self):
        # Create and play some processes
        procs = []
        for proc_class in test_utils.TEST_PROCESSES + test_utils.TEST_EXCEPTION_PROCESSES:
            proc = proc_class(loop=self.loop)
            proc.play()
            procs.append(proc)

        # Check that they all run
        gathered = plum.gather(*[proc.future() for proc in procs])
        plum.run_until_complete(gathered, self.loop)

    def test_recreate_from(self):
        proc = test_utils.DummyProcess()
        p2 = self._assert_same(proc)
        self._procs_same(proc, p2)

        proc.play()
        p2 = self._assert_same(proc)
        self._procs_same(proc, p2)

        proc.finish()
        p2 = self._assert_same(proc)
        self._procs_same(proc, p2)

    def _assert_same(self, proc):
        return plum.Bundle(proc).unbundle(loop=proc.loop())

    def _procs_same(self, p1, p2):
        self.assertEqual(p1.state, p2.state)
        if p1.state == ProcessState.FINISHED:
            self.assertEqual(p1.result(), p2.result())

    def _check_process_against_snapshot(self, snapshot, proc):
        self.assertEqual(snapshot.state, proc.state)

        new_bundle = plum.Bundle()
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


class TestProcessNamespace(utils.TestCaseWithLoop):

    def test_namespaced_process(self):
        """
        Test that inputs in nested namespaces are properly validated and the returned
        Process inputs data structure consists of nested AttributesFrozenDict instances
        """
        class NameSpacedProcess(Process):

            @classmethod
            def define(cls, spec):
                super(NameSpacedProcess, cls).define(spec)
                spec.input('some.name.space.a', valid_type=int)

        proc = NameSpacedProcess(inputs={'some': {'name': {'space': {'a': 5}}}})

        # Test that the namespaced inputs are AttributesFrozendict
        self.assertIsInstance(proc.inputs, AttributesFrozendict)
        self.assertIsInstance(proc.inputs.some, AttributesFrozendict)
        self.assertIsInstance(proc.inputs.some.name, AttributesFrozendict)
        self.assertIsInstance(proc.inputs.some.name.space, AttributesFrozendict)

        # Test that the input node is in the inputs of the process
        input_value = proc.inputs.some.name.space.a
        self.assertTrue(isinstance(input_value, int))
        self.assertEquals(input_value, 5)


class TestProcessEvents(utils.TestCaseWithLoop):
    def setUp(self):
        super(TestProcessEvents, self).setUp()
        self.proc = test_utils.DummyProcessWithOutput()

    def tearDown(self):
        super(TestProcessEvents, self).tearDown()

    def test_basic_events(self):
        events_tester = test_utils.ProcessListenerTester(
            self.proc, ('running', 'output_emitted', 'finished'),
            self.loop.stop)
        self.proc.play()

        utils.run_loop_with_timeout(self.loop)
        self.assertSetEqual(events_tester.called, events_tester.expected_events)

    def test_cancelled(self):
        events_tester = test_utils.ProcessListenerTester(self.proc, ('cancelled',), self.loop.stop)
        self.proc.cancel()
        utils.run_loop_with_timeout(self.loop)

        # Do the checks
        self.assertTrue(self.proc.cancelled())
        self.assertSetEqual(events_tester.called, events_tester.expected_events)

    def test_failed(self):
        events_tester = test_utils.ProcessListenerTester(self.proc, ('failed',), self.loop.stop)
        self.proc.fail(RuntimeError('See ya later suckers'))
        utils.run_loop_with_timeout(self.loop)

        # Do the checks
        self.assertIsNotNone(self.proc.exception())
        self.assertSetEqual(events_tester.called, events_tester.expected_events)

    def test_paused(self):
        events_tester = test_utils.ProcessListenerTester(self.proc, ('paused',), self.loop.stop)
        self.proc.pause()
        utils.run_loop_with_timeout(self.loop)

        # Do the checks
        self.assertSetEqual(events_tester.called, events_tester.expected_events)

    def test_broadcast(self):
        communicator = kiwipy.LocalCommunicator()

        messages = []

        def on_broadcast_receive(**msg):
            messages.append(msg)

        communicator.add_broadcast_subscriber(on_broadcast_receive)
        proc = test_utils.DummyProcess(communicator=communicator)
        proc.execute()

        expected_subjects = []
        for i, state in enumerate(test_utils.DummyProcess.EXPECTED_STATE_SEQUENCE):
            from_state = test_utils.DummyProcess.EXPECTED_STATE_SEQUENCE[i - 1].value if i != 0 else None
            expected_subjects.append(
                "state_changed.{}.{}".format(from_state, state.value))

        for i, message in enumerate(messages):
            self.assertEqual(message['subject'], expected_subjects[i])


class _RestartProcess(test_utils.WaitForSignalProcess):
    @classmethod
    def define(cls, spec):
        super(_RestartProcess, cls).define(spec)
        spec.outputs.dynamic = True

    def last_step(self):
        self.out("finished", True)