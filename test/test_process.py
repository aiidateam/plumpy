import plum
from past.builtins import basestring
from plum import Process, ProcessState
from plum.test_utils import check_process_against_snapshots
from plum import test_utils
from plum.test_utils import ProcessListenerTester, NewLoopProcess
from plum import process

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

                spec.dynamic_input()

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
        # Not all that much we can test for, but check if it's a string at
        # least
        for proc_class in test_utils.TEST_PROCESSES:
            desc = proc_class.get_description()
            self.assertIsInstance(desc, str)

        # Dummy process should at least use the docstring as part of the
        # description and so it shouldn't be empty
        desc = test_utils.DummyProcess.get_description()
        self.assertNotEqual(desc, "")

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
        BundleKeys = process.BundleKeys
        proc = test_utils.DummyProcessWithOutput()

        saver = test_utils.ProcessSaver(proc)
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
        for proc_class in test_utils.TEST_PROCESSES:
            proc = proc_class()
            saver = test_utils.ProcessSaver(proc)
            saver.capture()
            self.assertEqual(proc.state, ProcessState.FINISHED)
            self.assertTrue(
                check_process_against_snapshots(self.loop, proc_class, saver.snapshots)
            )

    def test_saving_each_step_interleaved(self):
        for ProcClass in test_utils.TEST_PROCESSES:
            proc = ProcClass()
            saver = test_utils.ProcessSaver(proc)
            saver.capture()

            self.assertTrue(
                check_process_against_snapshots(self.loop, ProcClass, saver.snapshots)
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


class TestProcessEvents(utils.TestCaseWithLoop):
    def setUp(self):
        super(TestProcessEvents, self).setUp()
        self.proc = test_utils.DummyProcessWithOutput()

    def tearDown(self):
        super(TestProcessEvents, self).tearDown()

    def test_basic_events(self):
        events_tester = ProcessListenerTester(
            self.proc, ('running', 'output_emitted', 'finished'),
            self.loop.stop)
        self.proc.play()

        utils.run_loop_with_timeout(self.loop)
        self.assertSetEqual(events_tester.called, events_tester.expected_events)

    def test_cancelled(self):
        events_tester = ProcessListenerTester(self.proc, ('cancelled',), self.loop.stop)
        self.proc.cancel()
        utils.run_loop_with_timeout(self.loop)

        # Do the checks
        self.assertTrue(self.proc.cancelled())
        self.assertSetEqual(events_tester.called, events_tester.expected_events)

    def test_failed(self):
        events_tester = ProcessListenerTester(self.proc, ('failed',), self.loop.stop)
        self.proc.fail(RuntimeError('See ya later suckers'))
        utils.run_loop_with_timeout(self.loop)

        # Do the checks
        self.assertIsNotNone(self.proc.exception())
        self.assertSetEqual(events_tester.called, events_tester.expected_events)

    def test_paused(self):
        events_tester = ProcessListenerTester(self.proc, ('paused',), self.loop.stop)
        self.proc.pause()
        utils.run_loop_with_timeout(self.loop)

        # Do the checks
        self.assertSetEqual(events_tester.called, events_tester.expected_events)


class _RestartProcess(test_utils.WaitForSignalProcess):
    @classmethod
    def define(cls, spec):
        super(_RestartProcess, cls).define(spec)
        spec.dynamic_output()

    def last_step(self):
        self.out("finished", True)


class TestExposeProcess(utils.TestCaseWithLoop):

    def setUp(self):
        super(TestExposeProcess, self).setUp()

        class SimpleProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(SimpleProcess, cls).define(spec)
                spec.input('a', valid_type=int, required=True)
                spec.input('b', valid_type=int, required=True)

        self.SimpleProcess = SimpleProcess

    def test_expose_duplicate_unnamespaced(self):
        """
        As long as separate namespaces are used, the same Process should be
        able to be exposed more than once
        """
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess)
                spec.expose_inputs(SimpleProcess, namespace='beta')

            def _run(self, **kwargs):
                assert self.inputs['a'] == 1
                assert self.inputs['b'] == 2
                assert self.inputs.beta['a'] == 3
                assert self.inputs.beta['b'] == 4
                SimpleProcess(inputs=self.exposed_inputs(SimpleProcess)).execute()
                SimpleProcess(inputs=self.exposed_inputs(SimpleProcess, namespace='beta')).execute()

        ExposeProcess(inputs={'a': 1, 'b': 2, 'beta': {'a': 3, 'b': 4}}).execute()

    def test_expose_duplicate_namespaced(self):
        """
        As long as separate namespaces are used, the same Process should be
        able to be exposed more than once
        """
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess, namespace='alef')
                spec.expose_inputs(SimpleProcess, namespace='beta')

            def _run(self, **kwargs):
                assert self.inputs.alef['a'] == 1
                assert self.inputs.alef['b'] == 2
                assert self.inputs.beta['a'] == 3
                assert self.inputs.beta['b'] == 4
                SimpleProcess(inputs=self.exposed_inputs(SimpleProcess, namespace='alef')).execute()
                SimpleProcess(inputs=self.exposed_inputs(SimpleProcess, namespace='beta')).execute()

        ExposeProcess(inputs={'alef': {'a': 1, 'b': 2}, 'beta': {'a': 3, 'b': 4}}).execute()

    def test_expose_pass_same_dictionary(self):
        """
        Pass the same dictionary to two different namespaces.
        """
        loop = self.loop
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess, namespace='alef')
                spec.expose_inputs(SimpleProcess, namespace='beta')

            def _run(self, **kwargs):
                assert self.inputs.alef['a'] == 1
                assert self.inputs.alef['b'] == 2
                assert self.inputs.beta['a'] == 1
                assert self.inputs.beta['b'] == 2
                SimpleProcess(inputs=self.exposed_inputs(SimpleProcess, namespace='alef')).execute()
                SimpleProcess(inputs=self.exposed_inputs(SimpleProcess, namespace='beta')).execute()

        inputs = {'a': 1, 'b': 2}
        ExposeProcess(inputs={'alef': inputs, 'beta': inputs}).execute()


class TestNestedUnnamespacedExposedProcess(utils.TestCaseWithLoop):

    def setUp(self):
        super(TestNestedUnnamespacedExposedProcess, self).setUp()

        class BaseProcess(Process):
            @classmethod
            def define(cls, spec):
                super(BaseProcess, cls).define(spec)
                spec.input('a', valid_type=int, required=True)
                spec.input('b', valid_type=int, default=0)

        class SubProcess(Process):
            @classmethod
            def define(cls, spec):
                super(SubProcess, cls).define(spec)
                spec.expose_inputs(BaseProcess)
                spec.input('c', valid_type=int, required=True)
                spec.input('d', valid_type=int, default=0)

        class ParentProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ParentProcess, cls).define(spec)
                spec.expose_inputs(SubProcess)
                spec.input('e', valid_type=int, required=True)
                spec.input('f', valid_type=int, default=0)

        self.BaseProcess = BaseProcess
        self.SubProcess = SubProcess
        self.ParentProcess = ParentProcess

    def test_base_process_valid_input(self):
        self.BaseProcess(inputs={'a': 0, 'b': 1})

    def test_sub_process_valid_input(self):
        self.SubProcess(inputs={'a': 0, 'b': 1, 'c': 2, 'd': 3})

    def test_parent_process_valid_input(self):
        self.ParentProcess(inputs={'a': 0, 'b': 1, 'c': 2, 'd': 3, 'e': 4, 'f': 5})

    def test_base_process_missing_input(self):
        with self.assertRaises(ValueError):
            self.BaseProcess(inputs={'b': 1})

    def test_sub_process_missing_input(self):
        with self.assertRaises(ValueError):
            self.SubProcess(inputs={'b': 1})

    def test_parent_process_missing_input(self):
        with self.assertRaises(ValueError):
            self.ParentProcess(inputs={'b': 1})

    def test_base_process_invalid_type_input(self):
        with self.assertRaises(ValueError):
            self.BaseProcess(inputs={'a': 0, 'b': 'string'})

    def test_sub_process_invalid_type_input(self):
        with self.assertRaises(ValueError):
            self.SubProcess(inputs={'a': 0, 'b': 1, 'c': 2, 'd': 'string'})

    def test_parent_process_invalid_type_input(self):
        with self.assertRaises(ValueError):
            self.ParentProcess(inputs={'a': 0, 'b': 1, 'c': 2, 'd': 3, 'e': 4, 'f': 'string'})


class TestNestedNamespacedExposedProcess(utils.TestCaseWithLoop):

    def setUp(self):
        super(TestNestedNamespacedExposedProcess, self).setUp()

        class BaseProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(BaseProcess, cls).define(spec)
                spec.input('a', valid_type=int, required=True)
                spec.input('b', valid_type=int, default=0)

        class SubProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(SubProcess, cls).define(spec)
                spec.expose_inputs(BaseProcess, namespace='base')
                spec.input('c', valid_type=int, required=True)
                spec.input('d', valid_type=int, default=0)

        class ParentProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(ParentProcess, cls).define(spec)
                spec.expose_inputs(SubProcess, namespace='sub')
                spec.input('e', valid_type=int, required=True)
                spec.input('f', valid_type=int, default=0)

        self.BaseProcess = BaseProcess
        self.SubProcess = SubProcess
        self.ParentProcess = ParentProcess

    def test_sub_process_valid_input(self):
        self.SubProcess(inputs={'base': {'a': 0, 'b': 1}, 'c': 2})

    def test_parent_process_valid_input(self):
        self.ParentProcess(inputs={'sub': {'base': {'a': 0, 'b': 1}, 'c': 2}, 'e': 4})

    def test_sub_process_missing_input(self):
        with self.assertRaises(ValueError):
            self.SubProcess(inputs={'c': 2})

    def test_parent_process_missing_input(self):
        with self.assertRaises(ValueError):
            self.ParentProcess(inputs={'e': 4})


class TestExposeProcessDynamicInput(utils.TestCaseWithLoop):
    """
    If a Process exposes the inputs of a sub Process that has dynamic input enabled
    then the input validation should correctly allow for this. Vice versa additional
    inputs in the absence of dynamic input should fail at input validation.
    """

    def setUp(self):
        super(TestExposeProcessDynamicInput, self).setUp()

        class SimpleNonDynamicProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(SimpleNonDynamicProcess, cls).define(spec)
                spec.input('a', valid_type=int, required=True)
                spec.input('b', valid_type=int, required=True)

        class ExposeNonDynamicProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(ExposeNonDynamicProcess, cls).define(spec)
                spec.expose_inputs(SimpleNonDynamicProcess, namespace='simple')
                spec.input('c', valid_type=int, required=True)

        class SimpleDynamicProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(SimpleDynamicProcess, cls).define(spec)
                spec.input('a', valid_type=int, required=True)
                spec.input('b', valid_type=int, required=True)
                spec.dynamic_input()

        class ExposeDynamicProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(ExposeDynamicProcess, cls).define(spec)
                spec.expose_inputs(SimpleDynamicProcess, namespace='simple')
                spec.input('c', valid_type=int, required=True)

        self.SimpleNonDynamicProcess = SimpleNonDynamicProcess
        self.ExposeNonDynamicProcess = ExposeNonDynamicProcess
        self.SimpleDynamicProcess = SimpleDynamicProcess
        self.ExposeDynamicProcess = ExposeDynamicProcess

    def test_expose_non_dynamic_input_valid(self):
        """
        When exposing process without dynamic input, any implicit inputs should
        already be not accepted by the input validation at the exposing process level
        """
        SimpleNonDynamicProcess = self.SimpleNonDynamicProcess
        ExposeNonDynamicProcess = self.ExposeNonDynamicProcess

        ExposeNonDynamicProcess(inputs={'c': 1, 'simple': {'a': 3, 'b': 4}}).execute()

        with self.assertRaises(ValueError):
            ExposeNonDynamicProcess(inputs={'c': 1, 'simple': {'a': 3, 'b': 4, 'tortoise': 'shell'}}).execute()

    def test_expose_dynamic_input_valid(self):
        """
        When exposing process with dynamic input, any implicit inputs should
        be accepted by input validation of the the exposing process
        """
        SimpleDynamicProcess = self.SimpleDynamicProcess
        ExposeDynamicProcess = self.ExposeDynamicProcess

        ExposeDynamicProcess(inputs={'c': 1, 'simple': {'a': 3, 'b': 4}}).execute()
        ExposeDynamicProcess(inputs={'c': 1, 'simple': {'a': 3, 'b': 4, 'tortoise': 'shell'}}).execute()


class TestExcludeExposeProcess(utils.TestCaseWithLoop):

    def setUp(self):
        super(TestExcludeExposeProcess, self).setUp()

        class SimpleProcess(Process):
            @classmethod
            def define(cls, spec):
                super(SimpleProcess, cls).define(spec)
                spec.input('a', valid_type=int, required=True)
                spec.input('b', valid_type=int, required=False)

        self.SimpleProcess = SimpleProcess

    def test_exclude_valid(self):
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess, exclude=('b',))
                spec.input('c', valid_type=int, required=True)

        ExposeProcess(inputs={'a': 1, 'c': 3})

    def test_exclude_invalid(self):
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess, exclude=('a',))

        with self.assertRaises(ValueError):
            ExposeProcess(inputs={'b': 2, 'c': 3})

    def test_exclude_same_input_in_parent(self):
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess, exclude=('a',))
                spec.input('a', valid_type=basestring)

            def _run(self, **kwargs):
                inputs = self.exposed_inputs(SimpleProcess)
                inputs['a'] = 1
                proc = SimpleProcess(inputs=inputs)

        ExposeProcess(inputs={'a': '1', 'b': 2})


class TestIncludeExposeProcess(utils.TestCaseWithLoop):

    def setUp(self):
        super(TestIncludeExposeProcess, self).setUp()

        class SimpleProcess(Process):
            @classmethod
            def define(cls, spec):
                super(SimpleProcess, cls).define(spec)
                spec.input('a', valid_type=int, required=True)
                spec.input('b', valid_type=int, required=False)

        self.SimpleProcess = SimpleProcess

    def test_include_none(self):
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess, include=[])
                spec.input('c', valid_type=int, required=True)

            def _run(self, **kwargs):
                inputs = self.exposed_inputs(SimpleProcess)
                inputs['a'] = 1
                inputs['b'] = 2
                SimpleProcess(inputs=inputs)

    def test_include_one(self):
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess, include=['a'])
                spec.input('c', valid_type=int, required=True)

            def _run(self, **kwargs):
                inputs = self.exposed_inputs(SimpleProcess)
                inputs['b'] = 2
                SimpleProcess(inputs=inputs)

        ExposeProcess(inputs={'a': 1, 'c': 3})


class TestUnionInputsExposeProcess(utils.TestCaseWithLoop):

    def setUp(self):
        super(TestUnionInputsExposeProcess, self).setUp()

        class SubOneProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(SubOneProcess, cls).define(spec)
                spec.input('common', valid_type=int, required=True)
                spec.input('sub_one', valid_type=int, required=True)

            def _run(self, **kwargs):
                assert self.inputs['common'] == 1
                assert self.inputs['sub_one'] == 2

        class SubTwoProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(SubTwoProcess, cls).define(spec)
                spec.input('common', valid_type=int, required=True)
                spec.input('sub_two', valid_type=int, required=True)

            def _run(self, **kwargs):
                assert self.inputs['common'] == 1
                assert self.inputs['sub_two'] == 3

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SubOneProcess)
                spec.expose_inputs(SubTwoProcess)

            def _run(self, **kwargs):
                SubOneProcess(inputs=self.exposed_inputs(SubOneProcess)).execute()
                SubTwoProcess(inputs=self.exposed_inputs(SubTwoProcess)).execute()

        self.SubOneProcess = SubOneProcess
        self.SubTwoProcess = SubTwoProcess
        self.ExposeProcess = ExposeProcess

    def test_inputs_union_valid(self):
        self.ExposeProcess(inputs={'common': 1, 'sub_one': 2, 'sub_two': 3}).execute()

    def test_inputs_union_invalid(self):
        with self.assertRaises(ValueError):
            self.ExposeProcess(inputs={'sub_one': 2, 'sub_two': 3}).execute()


class TestAgglomerateExposeProcess(utils.TestCaseWithLoop):
    """
    Often one wants to run multiple instances of a certain Process, where some but
    not all the inputs will be the same or "common". By using a combination of include and
    exclude on the same Process, the user can define separate namespaces for the specific
    inputs, while exposing the shared or common inputs on the base level namespace. The
    method exposed_inputs will by default agglomerate inputs that belong to the SubProcess
    starting from the base level and moving down the specified namespaces, overriding duplicate
    inputs as they are found.

    The exposed_inputs provides the flag 'agglomerate' which can be set to False to turn off
    this behavior and only return the inputs in the specified namespace
    """

    def setUp(self):
        super(TestAgglomerateExposeProcess, self).setUp()

        class SubProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(SubProcess, cls).define(spec)
                spec.input('common', valid_type=int, required=True)
                spec.input('specific_a', valid_type=int, required=True)
                spec.input('specific_b', valid_type=int, required=True)

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SubProcess, include=('common',))
                spec.expose_inputs(SubProcess, namespace='sub_a', exclude=('common',))
                spec.expose_inputs(SubProcess, namespace='sub_b', exclude=('common',))

            def _run(self, **kwargs):
                SubProcess(inputs=self.exposed_inputs(SubProcess, namespace='sub_a')).execute()
                SubProcess(inputs=self.exposed_inputs(SubProcess, namespace='sub_b')).execute()

        self.SubProcess = SubProcess
        self.ExposeProcess = ExposeProcess

    def test_inputs_union_valid(self):
        inputs = {
            'common': 1,
            'sub_a': {
                'specific_a': 2,
                'specific_b': 3
            },
            'sub_b': {
                'specific_a': 4,
                'specific_b': 5
            }
        }
        self.ExposeProcess(inputs=inputs).execute()

    def test_inputs_union_invalid(self):
        inputs = {
            'sub_a': {
                'specific_a': 2,
                'specific_b': 3
            },
            'sub_b': {
                'specific_a': 4,
                'specific_b': 5
            }
        }
        with self.assertRaises(ValueError):
            self.ExposeProcess(inputs=inputs).execute()


class TestNonAgglomerateExposeProcess(utils.TestCaseWithLoop):
    """
    Example where the default agglomerate behavior of exposed_inputs is undesirable and can be
    switched off by setting the flag agglomerate to False. The SubProcess shares an input with
    the parent processs, but unlike for the ExposeProcess, for the SubProcess it is not required.
    A user might for that reason not want to pass the common input to the SubProcess.
    """

    def setUp(self):
        super(TestNonAgglomerateExposeProcess, self).setUp()

        class SubProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(SubProcess, cls).define(spec)
                spec.input('specific_a', valid_type=int, required=True)
                spec.input('specific_b', valid_type=int, required=True)
                spec.input('common', valid_type=int, required=False)

            def _run(self, **kwargs):
                assert 'common' not in self.inputs

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SubProcess, namespace='sub')
                spec.input('common', valid_type=int, required=True)

            def _run(self, **kwargs):
                SubProcess(inputs=self.exposed_inputs(SubProcess, namespace='sub', agglomerate=False)).execute()

        self.SubProcess = SubProcess
        self.ExposeProcess = ExposeProcess

    def test_valid_input_non_agglomerate(self):
        inputs = {
            'common': 1,
            'sub': {
                'specific_a': 2,
                'specific_b': 3
            },
        }
        self.ExposeProcess(inputs=inputs).execute()