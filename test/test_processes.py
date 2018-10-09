from __future__ import absolute_import
import kiwipy
import plumpy
from plumpy import Process, ProcessState, test_utils, BundleKeys
from plumpy.utils import AttributesFrozendict
from tornado import gen, testing
import tornado.gen

import six
from six.moves import range
from six.moves import zip

from . import utils


class AsyncTestCase(testing.AsyncTestCase):
    """Out custom version of the async test case from tornado"""

    def setUp(self):
        super(AsyncTestCase, self).setUp()
        self.loop = self.io_loop


class ForgetToCallParent(plumpy.Process):

    def __init__(self, forget_on):
        super(ForgetToCallParent, self).__init__()
        self.forget_on = forget_on

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


class TestProcess(AsyncTestCase):

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
            pass

        class WithDynamic(Process):

            @classmethod
            def define(cls, spec):
                super(WithDynamic, cls).define(spec)
                spec.inputs.dynamic = True

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
        self.assertEqual(process.pid, 5)

        # Test using string as pid
        process = test_utils.DummyProcessWithOutput(pid='a')
        self.assertEqual(process.pid, 'a')

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
        self.assertIsInstance(desc_with_spec['description'], six.string_types)

    def test_logging(self):

        class LoggerTester(Process):

            def run(self, **kwargs):
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

        listener = plumpy.ProcessListener()
        listener.on_process_waiting = lambda proc: proc.resume()

        proc.add_process_listener(listener)
        self.loop.run_sync(proc.step_until_terminated)

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
        loop = self.loop
        loop.add_callback(proc.step_until_terminated)

        @gen.coroutine
        def async_test():
            yield test_utils.run_until_waiting(proc)
            self.assertEqual(proc.state, ProcessState.WAITING)

            result = yield proc.pause()
            self.assertTrue(result)
            self.assertTrue(proc.paused)

            result = proc.play()
            self.assertTrue(result)
            self.assertFalse(proc.paused)

            proc.resume()
            # Wait until the process is terminated
            yield proc.future()

            # Check it's done
            self.assertTrue(proc.done())
            self.assertEqual(proc.state, ProcessState.FINISHED)

        loop.run_sync(async_test)

    def test_pause_play_status_messaging(self):
        """
        Test the setting of a processes' status through pause and play works correctly.

        Any process can have its status set to a given message. When pausing, a pause message can be set for the
        status, which should store the current status, which should be restored, once the process is played again.
        """
        PLAY_STATUS = 'process was played by Hans Klok'
        PAUSE_STATUS = 'process was paused by Evel Knievel'

        proc = test_utils.WaitForSignalProcess()
        proc.set_status(PLAY_STATUS)
        loop = self.loop
        loop.add_callback(proc.step_until_terminated)

        @gen.coroutine
        def async_test():
            yield test_utils.run_until_waiting(proc)
            self.assertEqual(proc.state, ProcessState.WAITING)

            result = yield proc.pause(PAUSE_STATUS)
            self.assertTrue(result)
            self.assertTrue(proc.paused)
            self.assertEqual(proc.status, PAUSE_STATUS)

            result = proc.play()
            self.assertEqual(proc.status, PLAY_STATUS)
            self.assertIsNone(proc._pre_paused_status)

            proc.resume()
            # Wait until the process is terminated
            yield proc.future()

            # Check it's done
            self.assertTrue(proc.done())
            self.assertEqual(proc.state, ProcessState.FINISHED)

        loop.run_sync(async_test)

    def test_kill_in_run(self):

        class KillProcess(Process):
            after_kill = False

            def run(self, **kwargs):
                self.kill()
                # The following line should be executed because kill will not
                # interrupt execution of a method call in the RUNNING state
                self.after_kill = True

        proc = KillProcess()
        with self.assertRaises(plumpy.KilledError):
            proc.execute()

        self.assertTrue(proc.after_kill)
        self.assertEqual(proc.state, ProcessState.KILLED)

    def test_kill_when_paused_in_run(self):

        class PauseProcess(Process):

            def run(self, **kwargs):
                self.pause()
                self.kill()

        proc = PauseProcess()
        with self.assertRaises(plumpy.KilledError):
            proc.execute()

        self.assertEqual(proc.state, ProcessState.KILLED)

    def test_kill_when_paused(self):
        proc = test_utils.WaitForSignalProcess()

        @gen.coroutine
        def run_async(proc):
            yield test_utils.run_until_waiting(proc)

            saved_state = plumpy.Bundle(proc)

            result = yield proc.pause()
            self.assertTrue(result)
            self.assertTrue(proc.paused)

            # Kill the process
            proc.kill()

            with self.assertRaises(plumpy.KilledError):
                result = yield proc.future()

            self.assertEqual(proc.state, ProcessState.KILLED)

        self.loop.add_callback(proc.step_until_terminated)
        self.loop.run_sync(lambda: run_async(proc))

    @testing.gen_test
    def test_run_multiple(self):
        # Create and play some processes
        procs = []
        for proc_class in test_utils.TEST_PROCESSES:
            proc = proc_class(loop=self.loop)
            self.loop.add_callback(proc.step_until_terminated)
            procs.append(proc)

        # Check that they all run
        futures = [proc.future() for proc in procs]
        yield futures

        for future, proc_class in zip(futures, test_utils.TEST_PROCESSES):
            self.assertDictEqual(proc_class.EXPECTED_OUTPUTS, future.result())

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

        self.assertFalse(proc.successful())

    def test_unsuccessful_result(self):
        ERROR_CODE = 256

        class Proc(Process):

            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)

            def run(self):
                return plumpy.UnsuccessfulResult(ERROR_CODE)

        proc = Proc()
        proc.execute()

        self.assertEqual(proc.result(), ERROR_CODE)

    def test_pause_in_process(self):
        """ Test that we can pause and cancel that by playing within the process """

        test_case = self

        class TestPausePlay(plumpy.Process):

            def run(self):
                fut = self.pause()
                test_case.assertIsInstance(fut, plumpy.Future)

        listener = plumpy.ProcessListener()
        listener.on_process_paused = lambda _proc: self.loop.stop()

        proc = TestPausePlay()
        proc.add_process_listener(listener)

        self.loop.add_callback(proc.step_until_terminated)
        self.loop.start()
        self.assertTrue(proc.paused)
        self.assertEqual(plumpy.ProcessState.FINISHED, proc.state)

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

        self.loop.run_sync(proc.step_until_terminated)
        self.assertFalse(proc.paused)
        self.assertEqual(plumpy.ProcessState.FINISHED, proc.state)

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


class TestProcessSaving(AsyncTestCase):
    maxDiff = None

    @testing.gen_test
    def test_running_save_instance_state(self):
        nsync_comeback = SavePauseProc()
        self.loop.add_callback(nsync_comeback.step_until_terminated)

        yield test_utils.run_until_paused(nsync_comeback)

        # Create a checkpoint
        bundle = plumpy.Bundle(nsync_comeback)
        self.assertListEqual([SavePauseProc.run.__name__], nsync_comeback.steps_ran)

        nsync_comeback.play()
        yield nsync_comeback.future()

        self.assertListEqual([SavePauseProc.run.__name__, SavePauseProc.step2.__name__], nsync_comeback.steps_ran)

        proc_unbundled = bundle.unbundle()

        # At bundle time the Process was paused, the future of which will be persisted to the bundle.
        # As a result the process, recreated from that bundle, will also be paused and will have to be played
        proc_unbundled.play()
        self.assertEqual(0, len(proc_unbundled.steps_ran))
        yield proc_unbundled.step_until_terminated()
        self.assertEqual([SavePauseProc.step2.__name__], proc_unbundled.steps_ran)

        # self.loop.add_callback(nsync.step_until_terminated)
        # self.loop.run_sync(lambda: run_async(nsync))

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
            self.assertTrue(test_utils.check_process_against_snapshots(self.loop, proc_class, saver.snapshots))

    def test_saving_each_step_interleaved(self):
        for ProcClass in test_utils.TEST_PROCESSES:
            proc = ProcClass()
            saver = test_utils.ProcessSaver(proc)
            saver.capture()

            self.assertTrue(test_utils.check_process_against_snapshots(self.loop, ProcClass, saver.snapshots))

    @testing.gen_test
    def test_restart(self):
        proc = _RestartProcess()
        self.loop.add_callback(proc.step_until_terminated)

        yield test_utils.run_until_waiting(proc)

        # Save the state of the process
        saved_state = plumpy.Bundle(proc)

        # Load a process from the saved state
        loaded_proc = saved_state.unbundle()
        self.assertEqual(loaded_proc.state, ProcessState.WAITING)

        # Now resume it
        loaded_proc.resume()
        yield loaded_proc.step_until_terminated()
        self.assertEqual(loaded_proc.outputs, {'finished': True})

    @testing.gen_test
    def test_wait_save_continue(self):
        """ Test that process saved while in WAITING state restarts correctly when loaded """
        proc = test_utils.WaitForSignalProcess()
        self.loop.add_callback(proc.step_until_terminated)

        yield test_utils.run_until_waiting(proc)

        saved_state = plumpy.Bundle(proc)

        # Run the process to the end
        proc.resume()
        result1 = yield proc.future()

        # Load from saved state and run again
        proc2 = saved_state.unbundle(plumpy.LoadSaveContext(loop=self.loop))
        self.loop.add_callback(proc2.step_until_terminated)
        proc2.resume()
        result2 = yield proc2.future()

        # Check results match
        self.assertEqual(result1, result2)

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
        self.assertEqual(input_value, 5)

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
                spec.input('label', valid_type=six.string_types, required=False)
                spec.input('description', valid_type=six.string_types, required=False)
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


class TestProcessEvents(AsyncTestCase):

    def setUp(self):
        super(TestProcessEvents, self).setUp()
        self.proc = test_utils.DummyProcessWithOutput(loop=self.loop)

    @testing.gen_test
    def test_basic_events(self):
        events_tester = test_utils.ProcessListenerTester(
            process=self.proc, expected_events=('running', 'output_emitted', 'finished'))
        yield self.proc.step_until_terminated()
        self.assertSetEqual(events_tester.called, events_tester.expected_events)

    def test_killed(self):
        events_tester = test_utils.ProcessListenerTester(self.proc, ('killed',))
        self.assertTrue(self.proc.kill())

        # Do the checks
        self.assertTrue(self.proc.killed())
        self.assertSetEqual(events_tester.called, events_tester.expected_events)

    @testing.gen_test
    def test_excepted(self):
        proc = test_utils.ExceptionProcess()
        events_tester = test_utils.ProcessListenerTester(proc, (
            'excepted',
            'running',
            'output_emitted',
        ))
        with self.assertRaises(RuntimeError):
            yield proc.step_until_terminated()
            proc.result()

        # Do the checks
        self.assertIsNotNone(proc.exception())
        self.assertSetEqual(events_tester.called, events_tester.expected_events)

    def test_paused(self):
        events_tester = test_utils.ProcessListenerTester(self.proc, ('paused',))
        self.assertTrue(self.proc.pause())

        # Do the checks
        self.assertSetEqual(events_tester.called, events_tester.expected_events)

    @testing.gen_test
    def test_broadcast(self):
        communicator = kiwipy.LocalCommunicator()

        messages = []

        def on_broadcast_receive(_comm, body, sender, subject, correlation_id):
            messages.append({'body': body, 'subject': subject, 'sender': sender, 'correlation_id': correlation_id})

        communicator.add_broadcast_subscriber(on_broadcast_receive)
        proc = test_utils.DummyProcess(communicator=communicator)
        yield proc.step_until_terminated()

        expected_subjects = []
        for i, state in enumerate(test_utils.DummyProcess.EXPECTED_STATE_SEQUENCE):
            from_state = test_utils.DummyProcess.EXPECTED_STATE_SEQUENCE[i - 1].value if i != 0 else None
            expected_subjects.append("state_changed.{}.{}".format(from_state, state.value))

        for i, message in enumerate(messages):
            self.assertEqual(message['subject'], expected_subjects[i])


class _RestartProcess(test_utils.WaitForSignalProcess):

    @classmethod
    def define(cls, spec):
        super(_RestartProcess, cls).define(spec)
        spec.outputs.dynamic = True

    def last_step(self):
        self.out("finished", True)
