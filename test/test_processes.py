import kiwipy
import plumpy
from past.builtins import basestring
from plumpy import Process, ProcessState, test_utils, BundleKeys
from plumpy.utils import AttributesFrozendict
import tornado.gen

from . import utils


class ForgetToCallParent(plumpy.Process):
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

    def on_except(self, exception):
        if self.forget_on != 'except':
            super(ForgetToCallParent, self).on_except(exception)

    def on_finish(self, result, successful):
        if self.forget_on != 'finish':
            super(ForgetToCallParent, self).on_finish(result, successful)

    def on_kill(self, msg):
        if self.forget_on != 'kill':
            super(ForgetToCallParent, self).on_kill(msg)


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
            NoDynamic(inputs={'a': 5}).execute()

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

    def test_forget_to_call_parent_kill(self):
        with self.assertRaises(AssertionError):
            proc = ForgetToCallParent('kill')
            proc.kill()
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
        with self.assertRaises(RuntimeError):
            proc.execute()
        self.assertEqual(proc.state, ProcessState.EXCEPTED)

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

    def test_logging(self):
        class LoggerTester(Process):
            def _run(self, **kwargs):
                self.logger.info("Test")

        # TODO: Test giving a custom logger to see if it gets used
        proc = LoggerTester()
        proc.execute()

    def test_kill(self):
        proc = test_utils.DummyProcess(loop=self.loop)

        proc.kill('Farewell!')
        self.assertTrue(proc.killed())
        self.assertEqual(proc.killed_msg(), 'Farewell!')
        self.assertEqual(proc.state, ProcessState.KILLED)

    def test_wait_continue(self):
        proc = test_utils.WaitForSignalProcess()
        # Wait - Execute the process and wait until it is waiting
        proc.add_on_waiting_callback(lambda _: proc.pause())
        proc.execute()
        proc.resume()
        proc.execute()

        # Check it's done
        self.assertTrue(proc.done())
        self.assertEqual(proc.state, ProcessState.FINISHED)

    def test_exc_info(self):
        proc = test_utils.ExceptionProcess()
        try:
            proc.execute()
        except RuntimeError as e:
            self.assertEqual(proc.exception(), e)

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
        proc.add_on_waiting_callback(lambda _: proc.pause())
        proc.execute()
        self.assertEqual(proc.state, ProcessState.WAITING)

        result = proc.pause()
        self.assertTrue(result)
        self.assertTrue(proc.paused)

        result = proc.play()
        self.assertTrue(result)
        self.assertFalse(proc.paused)

        proc.resume()

        # Run
        proc.execute()

        # Check it's done
        self.assertTrue(proc.done())
        self.assertEqual(proc.state, ProcessState.FINISHED)

    def test_kill_in_run(self):
        class KillProcess(Process):
            after_kill = False

            def _run(self, **kwargs):
                self.kill()
                self.after_kill = True

        proc = KillProcess()
        with self.assertRaises(plumpy.KilledError):
            proc.execute()

        self.assertFalse(proc.after_kill)
        self.assertEqual(proc.state, ProcessState.KILLED)

    def test_run_multiple(self):
        # Create and play some processes
        procs = []
        for proc_class in test_utils.TEST_PROCESSES + test_utils.TEST_EXCEPTION_PROCESSES:
            proc = proc_class(loop=self.loop)
            self.loop.add_callback(proc.step_until_terminated)
            procs.append(proc)

        # Check that they all run
        gathered = plumpy.gather(*[proc.future() for proc in procs])
        plumpy.run_until_complete(gathered, self.loop)

    def test_invalid_output(self):
        class InvalidOutput(plumpy.Process):
            def run(self):
                self.out("invalid", 5)

        proc = InvalidOutput()
        with self.assertRaises(TypeError):
            proc.execute()

    def test_missing_output(self):
        proc = test_utils.MissingOutputProcess()

        with self.assertRaises(plumpy.InvalidStateError):
            proc.successful()

        proc.execute()

        self.assertEquals(proc.successful(), False)

    def test_unsuccessful_result(self):
        ERROR_CODE = 256

        class Proc(Process):
            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)

            def _run(self):
                return plumpy.UnsuccessfulResult(ERROR_CODE)

        proc = Proc()
        proc.execute()

        self.assertEquals(proc.result(), ERROR_CODE)

    def test_pause_play_in_process(self):
        """ Test that we can pause and cancel that by playing within the process """

        test_case = self

        class TestPausePlay(plumpy.Process):
            def run(self):
                fut = self.pause()
                test_case.assertIsInstance(fut, plumpy.Future)
                result = self.play()
                test_case.assertTrue(result)

        proc = TestPausePlay()
        proc.execute()
        self.assertEquals(plumpy.ProcessState.FINISHED, proc.state)

    def test_process_stack(self):
        test_case = self

        class StackTest(plumpy.Process):
            def run(self):
                test_case.assertIs(self, Process.current())

        proc = StackTest()
        proc.execute()

    def test_process_stack_multiple(self):
        """
        Run multiple and nested processes to make sure the process stack is always correct
        """
        test_case = self

        def test_nested(process):
            test_case.assertIs(process, Process.current())

        class StackTest(plumpy.Process):
            def run(self):
                test_case.assertIs(self, Process.current())
                test_nested(self)

        class ParentProcess(plumpy.Process):
            def run(self):
                test_case.assertIs(self, Process.current())
                StackTest().execute()

        to_run = []
        for _ in range(100):
            to_run.append(ParentProcess().step_until_terminated())

        self.loop.run_sync(tornado.gen.coroutine(lambda: (yield tornado.gen.multi(to_run))))

    def test_call_soon(self):
        class CallSoon(plumpy.Process):
            def run(self):
                self.call_soon(self.do_except)

            def do_except(self):
                raise RuntimeError("Breaking yo!")

        CallSoon().execute()


@plumpy.auto_persist('steps_ran')
class SavePauseProc(plumpy.Process):
    steps_ran = None

    def init(self):
        super(SavePauseProc, self).init()
        self.steps_ran = []

    def run(self):
        self.pause()
        self.steps_ran.append(self.run.__name__)
        return plumpy.Continue(self.step2)

    def step2(self):
        self.steps_ran.append(self.step2.__name__)


class TestProcessSaving(utils.TestCaseWithLoop):
    maxDiff = None

    def test_running_save_instance_state(self):
        proc = SavePauseProc()
        proc.execute()
        bundle = plumpy.Bundle(proc)
        self.assertListEqual([SavePauseProc.run.__name__], proc.steps_ran)
        proc.execute()
        self.assertListEqual([SavePauseProc.run.__name__, SavePauseProc.step2.__name__], proc.steps_ran)

        proc_unbundled = bundle.unbundle()
        self.assertEqual(0, len(proc_unbundled.steps_ran))
        proc_unbundled.execute()

        self.assertEqual([SavePauseProc.step2.__name__], proc_unbundled.steps_ran)

    def test_created_bundle(self):
        """
        Check that the bundle after just creating a process is as we expect
        """
        self._check_round_trip(test_utils.DummyProcess())

    def test_instance_state_with_outputs(self):
        proc = test_utils.DummyProcessWithOutput()

        saver = test_utils.ProcessSaver(proc)
        proc.execute()

        self._check_round_trip(proc)

        for bundle, outputs in zip(saver.snapshots, saver.outputs):
            # Check that it is a copy
            self.assertIsNot(outputs, bundle.get(BundleKeys.OUTPUTS, {}))
            # Check the contents are the same
            self.assertDictEqual(outputs, bundle.get(BundleKeys.OUTPUTS, {}))

        self.assertIsNot(proc.outputs, saver.snapshots[-1].get(BundleKeys.OUTPUTS, {}))

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

    def test_restart(self):
        proc = _RestartProcess()
        proc.add_on_waiting_callback(lambda _: proc.pause())
        proc.execute()

        # Save the state of the process
        saved_state = plumpy.Bundle(proc)

        # Load a process from the saved state
        proc = saved_state.unbundle()
        self.assertEqual(proc.state, ProcessState.WAITING)

        # Now resume it
        proc.resume()
        result = proc.execute()
        self.assertEqual(proc.outputs, {'finished': True})

    def test_wait_save_continue(self):
        """ Test that process saved while in WAITING state restarts correctly when loaded """
        proc = test_utils.WaitForSignalProcess()

        # Wait - Run the process until it enters the WAITING state
        proc.add_on_waiting_callback(lambda _: proc.pause())
        proc.execute()

        saved_state = plumpy.Bundle(proc)

        # Run the process to the end
        proc.resume()
        result = proc.execute()

        # Load from saved state and run again
        proc = saved_state.unbundle(plumpy.LoadSaveContext(loop=self.loop))
        proc.resume()
        result2 = proc.execute()

        # Check results match
        self.assertEqual(result, result2)

    def test_killed(self):
        proc = test_utils.DummyProcess()
        proc.kill()
        self.assertEqual(proc.state, plumpy.ProcessState.KILLED)
        self._check_round_trip(proc)

    def _check_round_trip(self, proc1):
        bundle1 = plumpy.Bundle(proc1)

        proc2 = bundle1.unbundle()
        bundle2 = plumpy.Bundle(proc2)

        self.assertEqual(proc1.pid, proc2.pid)
        self.assertDictEqual(bundle1, bundle2)


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

    def test_namespaced_process_inputs(self):
        """
        Test the parsed inputs for a process with namespace only contains expected dictionaries
        """

        class NameSpacedProcess(Process):

            @classmethod
            def define(cls, spec):
                super(NameSpacedProcess, cls).define(spec)
                spec.input('some.name.space.a', valid_type=int)
                spec.input('test', valid_type=int, default=6)
                spec.input('label', valid_type=basestring, required=False)
                spec.input('description', valid_type=basestring, required=False)
                spec.input('store_provenance', valid_type=bool, default=True)

        proc = NameSpacedProcess(inputs={'some': {'name': {'space': {'a': 5}}}})

        self.assertEqual(proc.inputs.test, 6)
        self.assertEqual(proc.inputs.store_provenance, True)
        self.assertEqual(proc.inputs.some.name.space.a, 5)

        self.assertTrue('label' not in proc.inputs)
        self.assertTrue('description' not in proc.inputs)

    def test_namespaced_process_dynamic(self):
        """
        Test that the input creation for processes with a dynamic nested port namespace is properly handled
        """
        namespace = 'name.space'

        class DummyDynamicProcess(Process):

            @classmethod
            def define(cls, spec):
                super(DummyDynamicProcess, cls).define(spec)
                spec.input_namespace(namespace)
                spec.inputs['name']['space'].dynamic = True
                spec.inputs['name']['space'].valid_type = int

        original_inputs = [1, 2, 3, 4]

        inputs = {'name': {'space': {str(l): l for l in original_inputs}}}
        p = DummyDynamicProcess(inputs=inputs)

        for label, value in p.inputs['name']['space'].items():
            self.assertTrue(label in inputs['name']['space'])
            self.assertEqual(int(label), value)
            original_inputs.remove(value)

        # Make sure there are no other inputs
        self.assertFalse(original_inputs)


class TestProcessEvents(utils.TestCaseWithLoop):
    def setUp(self):
        super(TestProcessEvents, self).setUp()
        self.proc = test_utils.DummyProcessWithOutput()

    def tearDown(self):
        super(TestProcessEvents, self).tearDown()

    def test_basic_events(self):
        events_tester = test_utils.ProcessListenerTester(
            process=self.proc,
            expected_events=('running', 'output_emitted', 'finished'),
            done_callback=self.loop.stop)
        self.loop.add_callback(self.proc.step_until_terminated)

        utils.run_loop_with_timeout(self.loop)
        self.assertSetEqual(events_tester.called, events_tester.expected_events)

    def test_killed(self):
        events_tester = test_utils.ProcessListenerTester(self.proc, ('killed',), self.loop.stop)
        self.proc.kill()
        utils.run_loop_with_timeout(self.loop)

        # Do the checks
        self.assertTrue(self.proc.killed())
        self.assertSetEqual(events_tester.called, events_tester.expected_events)

    def test_excepted(self):
        proc = test_utils.ExceptionProcess()
        events_tester = test_utils.ProcessListenerTester(proc, ('excepted', 'running', 'output_emitted',),
                                                         self.loop.stop)
        with self.assertRaises(RuntimeError):
            proc.execute()

        # Do the checks
        self.assertIsNotNone(proc.exception())
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
