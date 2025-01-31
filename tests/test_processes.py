# -*- coding: utf-8 -*-
"""Process tests"""

import asyncio
import enum

import pytest
from plumpy.futures import CancellableAction

import plumpy
from plumpy import BundleKeys, Process, ProcessState
from plumpy.message import MESSAGE_TEXT_KEY, MsgKill
from plumpy.persistence import Savable
from plumpy.utils import AttributesFrozendict
from . import utils


class ForgetToCallParent(plumpy.Process):
    def __init__(self, forget_on):
        super().__init__()
        self.forget_on = forget_on

    def on_create(self):
        if self.forget_on != 'create':
            super().on_create()

    def on_run(self):
        if self.forget_on != 'run':
            super().on_run()

    def on_except(self, exception):
        if self.forget_on != 'except':
            super().on_except(exception)

    def on_finish(self, result, successful):
        if self.forget_on != 'finish':
            super().on_finish(result, successful)

    def on_kill(self, msg):
        if self.forget_on != 'kill':
            super().on_kill(msg)


@pytest.mark.usefixtures('custom_event_loop_policy')
def test_process_is_savable():
    proc = utils.DummyProcess()
    assert isinstance(proc, Savable)

    # TODO: direct save load round trip regression


@pytest.mark.asyncio
async def test_process_scope():
    class ProcessTaskInterleave(plumpy.Process):
        async def task(self, steps: list):
            steps.append(f'[{self.pid}] started')
            assert plumpy.Process.current() is self
            steps.append(f'[{self.pid}] sleeping')
            await asyncio.sleep(0.1)
            assert plumpy.Process.current() is self
            steps.append(f'[{self.pid}] finishing')

    p1 = ProcessTaskInterleave()
    p2 = ProcessTaskInterleave()

    p1steps = []
    p2steps = []
    p1task = asyncio.ensure_future(p1._run_task(p1.task, p1steps))
    p2task = asyncio.ensure_future(p2._run_task(p2.task, p2steps))
    await p1task, p2task


class TestProcess:
    def test_spec(self):
        """
        Check that the references to specs are doing the right thing...
        """
        proc = utils.DummyProcess()
        assert utils.DummyProcess.spec() is not Process.spec()
        assert proc.spec() is utils.DummyProcess.spec()

        class Proc(utils.DummyProcess):
            pass

        assert Proc.spec() is not Process.spec()
        assert Proc.spec() is not utils.DummyProcess.spec()
        p = Proc()
        assert p.spec() is Proc.spec()

    def test_dynamic_inputs(self):
        class NoDynamic(Process):
            pass

        class WithDynamic(Process):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.inputs.dynamic = True

        with pytest.raises(ValueError):
            NoDynamic(inputs={'a': 5}).execute()

        proc = WithDynamic(inputs={'a': 5})
        proc.execute()

    def test_inputs(self):
        class Proc(Process):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('a')

        p = Proc({'a': 5})

        # Check that we can access the inputs after creating
        assert p.raw_inputs.a == 5
        with pytest.raises(AttributeError):
            p.raw_inputs.b

    def test_raw_inputs(self):
        """Test that the ``raw_inputs`` are not mutated by the ``Process`` constructor.

        Regression test for https://github.com/aiidateam/plumpy/issues/250
        """

        class Proc(Process):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('a')
                spec.input('nested.a')
                spec.input('nested.b', default='default-value')

        inputs = {'a': 5, 'nested': {'a': 'value'}}
        process = Proc(inputs)

        # Compare against a clone of the original inputs dictionary as the original is modified. It should not contain
        # the default value of the ``nested.b`` port.
        assert dict(process.raw_inputs) == {'a': 5, 'nested': {'a': 'value'}}

    def test_inputs_default(self):
        class Proc(utils.DummyProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('input', default=5, required=False)

        # Supply a value
        p = Proc(inputs={'input': 2})
        assert p.inputs['input'] == 2

        # Don't supply, use default
        p = Proc()
        assert p.inputs['input'] == 5

    def test_optional_namespace(self):
        """Process with an optional namespace should not have that in `self.inputs` if not explicitly passed."""

        class SomeProcess(Process):
            """Process with single dynamic optional namespace."""

            @classmethod
            def define(cls, spec):
                super(SomeProcess, cls).define(spec)
                spec.input_namespace('namespace', required=False, valid_type=int, dynamic=True)

        # If a value is specified for `namespace` it should be present in parsed inputs
        process = SomeProcess(inputs={'namespace': {'a': 1}})
        assert 'namespace' in process.inputs
        assert 'a' in process.inputs['namespace']
        assert process.inputs['namespace']['a'] == 1

        # If nothing is passed, it should not be present
        process = SomeProcess()
        assert 'namespace' not in process.inputs

        # However, if something is passed it should be there even if it is just an empty mapping
        process = SomeProcess(inputs={'namespace': {}})
        assert 'namespace' in process.inputs

        class SomeDefaultProcess(Process):
            """Process with single dynamic optional namespace, but with one concrete port with a default."""

            @classmethod
            def define(cls, spec):
                super(SomeDefaultProcess, cls).define(spec)
                spec.input_namespace('namespace', required=False, valid_type=int, dynamic=True)
                spec.input('namespace.b', default=5)

        # Even though `namespace` is optional and it is not explicitly passed as input, because the port `b` nested
        # within it has a default, the `namespace` mapping should be present in the parsed inputs.
        process = SomeDefaultProcess()
        assert 'namespace' in process.inputs
        assert 'b' in process.inputs['namespace']
        assert process.inputs['namespace']['b'] == 5

    def test_inputs_default_that_evaluate_to_false(self):
        for def_val in (True, False, 0, 1):

            class Proc(utils.DummyProcess):
                @classmethod
                def define(cls, spec):
                    super().define(spec)
                    spec.input('input', default=def_val)

            # Don't supply, use default
            p = Proc()
            assert 'input' in p.inputs
            assert p.inputs['input'] == def_val

    def test_nested_namespace_defaults(self):
        """Process with a default in a nested namespace should be created, even if top level namespace not supplied."""

        class SomeProcess(Process):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input_namespace('namespace', required=False)
                spec.input('namespace.sub', default=True)

        process = SomeProcess()
        assert 'sub' in process.inputs.namespace
        assert process.inputs.namespace.sub == True

    def test_raise_in_define(self):
        """Process which raises in its 'define' method. Check that the spec is not set."""

        class BrokenProcess(Process):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                raise ValueError

        with pytest.raises(ValueError):
            BrokenProcess.spec()
        # Check that the error is still raised when calling .spec()
        # a second time.
        with pytest.raises(ValueError):
            BrokenProcess.spec()

    def test_execute(self):
        proc = utils.DummyProcessWithOutput()
        proc.execute()

        assert proc.has_terminated()
        assert proc.state_label == ProcessState.FINISHED
        assert proc.outputs == {'default': 5}

    def test_run_from_class(self):
        # Test running through class method
        proc = utils.DummyProcessWithOutput()
        proc.execute()
        results = proc.outputs
        assert results['default'] == 5

    def test_forget_to_call_parent(self):
        for event in ('create', 'run', 'finish'):
            with pytest.raises(AssertionError):
                proc = ForgetToCallParent(event)
                proc.execute()

    def test_forget_to_call_parent_kill(self):
        proc = ForgetToCallParent('kill')
        proc.kill()
        assert proc.is_excepted

    def test_pid(self):
        # Test auto generation of pid
        process = utils.DummyProcessWithOutput()
        assert process.pid is not None

        # Test using integer as pid
        process = utils.DummyProcessWithOutput(pid=5)
        assert process.pid == 5

        # Test using string as pid
        process = utils.DummyProcessWithOutput(pid='a')
        assert process.pid == 'a'

    def test_exception(self):
        proc = utils.ExceptionProcess()
        with pytest.raises(RuntimeError):
            proc.execute()
        assert proc.state_label == ProcessState.EXCEPTED

    def test_run_kill(self):
        proc = utils.KillProcess()
        with pytest.raises(plumpy.KilledError, match='killed'):
            proc.execute()

    def test_get_description(self):
        class ProcWithoutSpec(Process):
            pass

        class ProcWithSpec(Process):
            """Process with a spec and a docstring"""

            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('a', default=1)

        for proc_class in utils.TEST_PROCESSES:
            desc = proc_class.get_description()
            assert isinstance(desc, dict)

        desc_with_spec = ProcWithSpec.get_description()
        desc_without_spec = ProcWithoutSpec.get_description()

        assert isinstance(desc_without_spec, dict)
        assert 'spec' in desc_without_spec
        assert 'description' not in desc_without_spec
        assert isinstance(desc_with_spec['spec'], dict)

        assert isinstance(desc_with_spec, dict)
        assert 'spec' in desc_with_spec
        assert 'description' in desc_with_spec
        assert isinstance(desc_with_spec['spec'], dict)
        assert isinstance(desc_with_spec['description'], str)

    def test_logging(self):
        class LoggerTester(Process):
            def run(self, **kwargs):
                self.logger.info('Test')

        # TODO: Test giving a custom logger to see if it gets used
        proc = LoggerTester()
        proc.execute()

    def test_kill(self):
        proc: Process = utils.DummyProcess()

        msg_text = 'Farewell!'
        proc.kill(msg_text=msg_text)
        assert proc.killed()
        assert proc.killed_msg()[MESSAGE_TEXT_KEY] == msg_text
        assert proc.state_label == ProcessState.KILLED

    def test_wait_continue(self):
        proc = utils.WaitForSignalProcess()
        # Wait - Execute the process and wait until it is waiting

        listener = plumpy.ProcessListener()
        listener.on_process_waiting = lambda proc: proc.resume()
        proc.add_process_listener(listener)

        proc.execute()

        # Check it's done
        assert proc.has_terminated()
        assert proc.state_label == ProcessState.FINISHED

    def test_exc_info(self):
        proc = utils.ExceptionProcess()
        try:
            proc.execute()
        except RuntimeError as e:
            assert proc.exception() == e

    def test_run_done(self):
        proc = utils.DummyProcess()
        proc.execute()
        assert proc.has_terminated()

    def test_wait_pause_play_resume(self):
        """
        Test that if you pause a process that and its awaitable finishes that it
        completes correctly when played again.
        """
        loop = asyncio.get_event_loop()
        proc = utils.WaitForSignalProcess()

        async def async_test():
            await utils.run_until_waiting(proc)
            assert proc.state_label == ProcessState.WAITING

            result = await proc.pause()
            assert result
            assert proc.paused

            result = proc.play()
            assert result
            assert not proc.paused

            proc.resume()
            # Wait until the process is terminated
            await proc.future()

            # Check it's done
            assert proc.has_terminated()
            assert proc.state_label == ProcessState.FINISHED

        loop.create_task(proc.step_until_terminated())
        loop.run_until_complete(async_test())

    def test_pause_play_status_messaging(self):
        """
        Test the setting of a processes' status through pause and play works correctly.

        Any process can have its status set to a given message. When pausing, a pause message can be set for the
        status, which should store the current status, which should be restored, once the process is played again.
        """
        PLAY_STATUS = 'process was played by Hans Klok'
        PAUSE_STATUS = 'process was paused by Evel Knievel'

        loop = asyncio.get_event_loop()
        proc = utils.WaitForSignalProcess()
        proc.set_status(PLAY_STATUS)

        async def async_test():
            await utils.run_until_waiting(proc)
            assert proc.state_label == ProcessState.WAITING

            result = await proc.pause(PAUSE_STATUS)
            assert result
            assert proc.paused
            assert proc.status == PAUSE_STATUS

            result = proc.play()
            assert proc.status == PLAY_STATUS
            assert proc._pre_paused_status is None

            proc.resume()
            # Wait until the process is terminated
            await proc.future()

        # Check it's done
        loop.create_task(proc.step_until_terminated())
        loop.run_until_complete(async_test())

        assert proc.has_terminated()
        assert proc.state_label == ProcessState.FINISHED

    def test_kill_in_run(self):
        class KillProcess(Process):
            after_kill = False

            def run(self, **kwargs):
                msg = MsgKill.new(text='killed')
                self.kill(msg)
                # The following line should be executed because kill will not
                # interrupt execution of a method call in the RUNNING state
                self.after_kill = True

        proc = KillProcess()
        with pytest.raises(plumpy.KilledError, match='killed'):
            proc.execute()

        assert proc.after_kill
        assert proc.state_label == ProcessState.KILLED

    def test_kill_when_paused_in_run(self):
        class PauseProcess(Process):
            def run(self, **kwargs):
                self.pause()
                self.kill()

        proc = PauseProcess()
        with pytest.raises(plumpy.KilledError):
            proc.execute()

        assert proc.state_label == ProcessState.KILLED

    def test_kill_when_paused(self):
        loop = asyncio.get_event_loop()
        proc = utils.WaitForSignalProcess()

        async def async_test():
            await utils.run_until_waiting(proc)

            saved_state = plumpy.Bundle(proc)

            result = await proc.pause()
            assert result
            assert proc.paused

            # Kill the process
            proc.kill()

            with pytest.raises(plumpy.KilledError):
                result = await proc.future()

        loop.create_task(proc.step_until_terminated())
        loop.run_until_complete(async_test())

        assert proc.state_label == ProcessState.KILLED

    def test_run_multiple(self):
        # Create and play some processes
        loop = asyncio.get_event_loop()

        procs = []
        for proc_class in utils.TEST_PROCESSES:
            proc = proc_class()
            loop.create_task(proc.step_until_terminated())
            procs.append(proc)

        tasks = asyncio.gather(*[p.future() for p in procs])
        results = loop.run_until_complete(tasks)

        for result, proc_class in zip(results, utils.TEST_PROCESSES):
            assert proc_class.EXPECTED_OUTPUTS == result

    def test_invalid_output(self):
        class InvalidOutput(plumpy.Process):
            def run(self):
                self.out('invalid', 5)

        proc = InvalidOutput()
        with pytest.raises(ValueError):
            proc.execute()

        assert proc.is_excepted

    def test_missing_output(self):
        proc = utils.MissingOutputProcess()

        with pytest.raises(plumpy.InvalidStateError):
            proc.successful()

        proc.execute()

        assert not proc.is_successful

    def test_unsuccessful_result(self):
        ERROR_CODE = 256

        class Proc(Process):
            @classmethod
            def define(cls, spec):
                super().define(spec)

            def run(self):
                return plumpy.UnsuccessfulResult(ERROR_CODE)

        proc = Proc()
        proc.execute()

        assert proc.result() == ERROR_CODE

    def test_pause_in_process(self):
        """Test that we can pause and cancel that by playing within the process"""
        test_case = self

        class TestPausePlay(plumpy.Process):
            def run(self):
                fut = self.pause()
                assert isinstance(fut, CancellableAction)

        loop = asyncio.get_event_loop()

        listener = plumpy.ProcessListener()
        listener.on_process_paused = lambda _proc: loop.stop()

        proc = TestPausePlay()
        proc.add_process_listener(listener)

        loop.create_task(proc.step_until_terminated())
        loop.run_forever()

        assert proc.paused
        assert proc.state_label == plumpy.ProcessState.FINISHED

    def test_pause_play_in_process(self):
        """Test that we can pause and play that by playing within the process"""

        class TestPausePlay(plumpy.Process):
            def run(self):
                fut = self.pause()
                assert isinstance(fut, CancellableAction)
                result = self.play()
                assert result

        proc = TestPausePlay()

        proc.execute()
        assert not proc.paused
        assert proc.state_label == plumpy.ProcessState.FINISHED

    def test_process_stack(self):
        class StackTest(plumpy.Process):
            def run(self):
                assert self is Process.current()

        proc = StackTest()
        proc.execute()

    @pytest.mark.usefixtures('custom_event_loop_policy')
    def test_process_stack_multiple(self):
        """
        Run multiple and nested processes to make sure the process stack is always correct
        """
        expect_true = []

        def test_nested(process):
            expect_true.append(process == Process.current())

        class StackTest(plumpy.Process):
            def run(self):
                # TODO: unexpected behaviour here
                # if assert error happend here not raise
                # it will be handled by try except clause in process
                # is there better way to handle this?
                expect_true.append(self == Process.current())
                test_nested(self)

        class ParentProcess(plumpy.Process):
            def run(self):
                expect_true.append(self == Process.current())
                StackTest().execute()

        to_run = []
        n_run = 3
        for _ in range(n_run):
            to_run.append(ParentProcess().step_until_terminated())

        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*to_run))

        assert all(expect_true)

        assert len(expect_true) == n_run * 3

    @pytest.mark.usefixtures('custom_event_loop_policy')
    def test_process_nested(self):
        """
        Run multiple and nested processes to make sure the process stack is always correct
        """

        class StackTest(plumpy.Process):
            def run(self):
                pass

        class ParentProcess(plumpy.Process):
            def run(self):
                StackTest().execute()

        ParentProcess().execute()

    def test_call_soon(self):
        class CallSoon(plumpy.Process):
            def run(self):
                self.call_soon(self.do_except)

            def do_except(self):
                raise RuntimeError('Breaking yo!')

        CallSoon().execute()

    def test_execute_twice(self):
        """Test a process that is executed once finished raises a ClosedError"""
        proc = utils.DummyProcess()
        proc.execute()
        with pytest.raises(plumpy.ClosedError):
            proc.execute()

    def test_exception_during_on_entered(self):
        """Test that an exception raised during ``on_entered`` will cause the process to be excepted."""

        class RaisingProcess(Process):
            def on_entered(self, from_state):
                if from_state is not None and from_state.LABEL == ProcessState.RUNNING:
                    raise RuntimeError('exception during on_entered')
                super().on_entered(from_state)

        process = RaisingProcess()

        with pytest.raises(RuntimeError):
            process.execute()

        assert not process.is_successful
        assert process.is_excepted
        assert str(process.exception()) == 'exception during on_entered'

    def test_exception_during_run(self):
        class RaisingProcess(Process):
            def run(self):
                raise RuntimeError('exception during run')

        process = RaisingProcess()

        with pytest.raises(RuntimeError):
            process.execute()

        assert process.is_excepted
        assert str(process.exception()) == 'exception during run'


@plumpy.auto_persist('steps_ran')
class SavePauseProc(plumpy.Process):
    steps_ran = None

    def init(self):
        super().init()
        self.steps_ran = []

    def run(self):
        self.pause()
        self.steps_ran.append(self.run.__name__)
        return plumpy.Continue(self.step2)

    def step2(self):
        self.steps_ran.append(self.step2.__name__)


class TestProcessSaving:
    maxDiff = None

    def test_running_save(self):
        loop = asyncio.get_event_loop()
        nsync_comeback = SavePauseProc()

        async def async_test():
            await utils.run_until_paused(nsync_comeback)

            # Create a checkpoint
            bundle = plumpy.Bundle(nsync_comeback)
            assert [SavePauseProc.run.__name__] == nsync_comeback.steps_ran

            nsync_comeback.play()
            await nsync_comeback.future()

            assert [SavePauseProc.run.__name__, SavePauseProc.step2.__name__] == nsync_comeback.steps_ran

            proc_unbundled = bundle.unbundle()

            # At bundle time the Process was paused, the future of which will be persisted to the bundle.
            # As a result the process, recreated from that bundle, will also be paused and will have to be played
            proc_unbundled.play()
            assert 0 == len(proc_unbundled.steps_ran)
            await proc_unbundled.step_until_terminated()
            assert [SavePauseProc.step2.__name__] == proc_unbundled.steps_ran

        loop.create_task(nsync_comeback.step_until_terminated())
        loop.run_until_complete(async_test())

    def test_save_future(self):
        """
        test `SavableFuture` is initialized with the event loop of process
        """
        loop = asyncio.new_event_loop()
        nsync_comeback = SavePauseProc(loop=loop)

        bundle = plumpy.Bundle(nsync_comeback)
        # if loop is not specified will use the default asyncio loop
        proc_unbundled = bundle.unbundle(plumpy.LoadSaveContext(loop=loop))

        async def async_test():
            await utils.run_until_paused(proc_unbundled)

            # here the future should be a SavableFuture in process loop
            proc_unbundled.play()
            await proc_unbundled.future()

            assert [SavePauseProc.run.__name__, SavePauseProc.step2.__name__] == proc_unbundled.steps_ran

        loop.create_task(proc_unbundled.step_until_terminated())
        loop.run_until_complete(async_test())

    def test_created_bundle(self):
        """
        Check that the bundle after just creating a process is as we expect
        """
        self._check_round_trip(utils.DummyProcess())

    def test_instance_state_with_outputs(self):
        proc = utils.DummyProcessWithOutput()

        saver = utils.ProcessSaver(proc)
        proc.execute()

        self._check_round_trip(proc)

        for bundle, outputs in zip(saver.snapshots, saver.outputs):
            # Check that it is a copy
            assert outputs is not bundle.get(BundleKeys.OUTPUTS, {})
            # Check the contents are the same
            # Remove the ``ProcessSaver`` instance that is only used for testing
            utils.compare_dictionaries(None, None, outputs, bundle.get(BundleKeys.OUTPUTS, {}), exclude={'_listeners'})

        assert proc.outputs is not saver.snapshots[-1].get(BundleKeys.OUTPUTS, {})

    def test_saving_each_step(self):
        loop = asyncio.get_event_loop()
        for proc_class in utils.TEST_PROCESSES:
            proc = proc_class()
            saver = utils.ProcessSaver(proc)
            saver.capture()
            assert proc.state_label == ProcessState.FINISHED
            assert utils.check_process_against_snapshots(loop, proc_class, saver.snapshots)

    def test_restart(self):
        loop = asyncio.get_event_loop()
        proc = _RestartProcess()

        async def async_test():
            await utils.run_until_waiting(proc)

            # Save the state of the process
            saved_state = plumpy.Bundle(proc)

            # Load a process from the saved state
            loaded_proc = saved_state.unbundle()
            assert loaded_proc.state_label == ProcessState.WAITING

            # Now resume it
            loaded_proc.resume()
            await loaded_proc.step_until_terminated()
            assert loaded_proc.outputs == {'finished': True}

        loop.create_task(proc.step_until_terminated())
        loop.run_until_complete(async_test())

    def test_double_restart(self):
        """Test that consecutive restarts do not cause any issues, this is tested for concurrency reasons."""
        loop = asyncio.get_event_loop()
        proc = _RestartProcess()

        async def async_test():
            await utils.run_until_waiting(proc)

            # Save the state of the process
            saved_state = plumpy.Bundle(proc)

            # Load a process from the saved state
            loaded_proc = saved_state.unbundle()
            assert loaded_proc.state_label == ProcessState.WAITING

            # Now resume it twice in succession
            loaded_proc.resume()
            loaded_proc.resume()

            await loaded_proc.step_until_terminated()
            assert loaded_proc.outputs == {'finished': True}

        loop.create_task(proc.step_until_terminated())
        loop.run_until_complete(async_test())

    def test_wait_save_continue(self):
        """Test that process saved while in WAITING state restarts correctly when loaded"""
        loop = asyncio.get_event_loop()
        proc = utils.WaitForSignalProcess()

        async def async_test():
            await utils.run_until_waiting(proc)

            saved_state = plumpy.Bundle(proc)

            # Run the process to the end
            proc.resume()
            result1 = await proc.future()

            # Load from saved state and run again
            loader = plumpy.get_object_loader()
            proc2 = saved_state.unbundle(plumpy.LoadSaveContext(loader))
            asyncio.ensure_future(proc2.step_until_terminated())
            proc2.resume()
            result2 = await proc2.future()

            # Check results match
            assert result1 == result2

        loop.create_task(proc.step_until_terminated())
        loop.run_until_complete(async_test())

    def test_killed(self):
        proc = utils.DummyProcess()
        proc.kill()
        assert proc.state_label == plumpy.ProcessState.KILLED
        self._check_round_trip(proc)

    def _check_round_trip(self, proc1):
        bundle1 = plumpy.Bundle(proc1)

        proc2 = bundle1.unbundle()
        bundle2 = plumpy.Bundle(proc2)

        assert proc1.pid == proc2.pid
        utils.compare_dictionaries(None, None, bundle1, bundle2, exclude={'_listeners'})


class TestProcessNamespace:
    def test_namespaced_process(self):
        """
        Test that inputs in nested namespaces are properly validated and the returned
        Process inputs data structure consists of nested AttributesFrozenDict instances
        """

        class NameSpacedProcess(Process):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('some.name.space.a', valid_type=int)

        proc = NameSpacedProcess(inputs={'some': {'name': {'space': {'a': 5}}}})

        # Test that the namespaced inputs are AttributesFrozendict
        assert isinstance(proc.inputs, AttributesFrozendict)
        assert isinstance(proc.inputs.some, AttributesFrozendict)
        assert isinstance(proc.inputs.some.name, AttributesFrozendict)
        assert isinstance(proc.inputs.some.name.space, AttributesFrozendict)

        # Test that the input node is in the inputs of the process
        input_value = proc.inputs.some.name.space.a
        assert isinstance(input_value, int)
        assert input_value == 5

    def test_namespaced_process_inputs(self):
        """
        Test the parsed inputs for a process with namespace only contains expected dictionaries
        """

        class NameSpacedProcess(Process):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('some.name.space.a', valid_type=int)
                spec.input('test', valid_type=int, default=6)
                spec.input('label', valid_type=str, required=False)
                spec.input('description', valid_type=str, required=False)
                spec.input('store_provenance', valid_type=bool, default=True)

        proc = NameSpacedProcess(inputs={'some': {'name': {'space': {'a': 5}}}})

        assert proc.inputs.test == 6
        assert proc.inputs.store_provenance == True
        assert proc.inputs.some.name.space.a == 5

        assert 'label' not in proc.inputs
        assert 'description' not in proc.inputs

    def test_namespaced_process_dynamic(self):
        """
        Test that the input creation for processes with a dynamic nested port namespace is properly handled
        """
        namespace = 'name.space'

        class DummyDynamicProcess(Process):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input_namespace(namespace)
                spec.inputs['name']['space'].dynamic = True
                spec.inputs['name']['space'].valid_type = int

        original_inputs = [1, 2, 3, 4]

        inputs = {'name': {'space': {str(l): l for l in original_inputs}}}
        proc = DummyDynamicProcess(inputs=inputs)

        for label, value in proc.inputs['name']['space'].items():
            assert label in inputs['name']['space']
            assert int(label) == value
            original_inputs.remove(value)

        # Make sure there are no other inputs
        assert not original_inputs

    def test_namespaced_process_outputs(self):
        """Test the output namespacing and validation."""
        namespace = 'integer'
        namespace_nested = f'{namespace}.nested'

        class OutputMode(enum.Enum):
            NONE = 0
            DYNAMIC_PORT_NAMESPACE = 1
            SINGLE_REQUIRED_PORT = 2
            BOTH_SINGLE_AND_NAMESPACE = 3

        class DummyDynamicProcess(Process):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('output_mode', valid_type=OutputMode, default=OutputMode.NONE)
                spec.output('required_bool', valid_type=bool)
                spec.output_namespace(namespace, valid_type=int, dynamic=True)

            def run(self):
                if self.inputs.output_mode == OutputMode.NONE:
                    pass
                elif self.inputs.output_mode == OutputMode.DYNAMIC_PORT_NAMESPACE:
                    self.out(namespace_nested + '.one', 1)
                    self.out(namespace_nested + '.two', 2)
                elif self.inputs.output_mode == OutputMode.SINGLE_REQUIRED_PORT:
                    self.out('required_bool', False)
                elif self.inputs.output_mode == OutputMode.BOTH_SINGLE_AND_NAMESPACE:
                    self.out('required_bool', False)
                    self.out(namespace_nested + '.one', 1)
                    self.out(namespace_nested + '.two', 2)

        # Run the process in default mode which should not add any outputs and therefore fail
        proc = DummyDynamicProcess()
        proc.execute()

        assert proc.state_label == ProcessState.FINISHED
        assert not proc.is_successful
        assert proc.outputs == {}

        # Attaching only namespaced ports should fail, because the required port is not added
        proc = DummyDynamicProcess(inputs={'output_mode': OutputMode.DYNAMIC_PORT_NAMESPACE})
        proc.execute()

        assert proc.state_label == ProcessState.FINISHED
        assert not proc.is_successful
        assert proc.outputs[namespace]['nested']['one'] == 1
        assert proc.outputs[namespace]['nested']['two'] == 2

        # Attaching only the single required top-level port should be fine
        proc = DummyDynamicProcess(inputs={'output_mode': OutputMode.SINGLE_REQUIRED_PORT})
        proc.execute()

        assert proc.state_label == ProcessState.FINISHED
        assert proc.is_successful
        assert proc.outputs['required_bool'] == False

        # Attaching both the required and namespaced ports should result in a successful termination
        proc = DummyDynamicProcess(inputs={'output_mode': OutputMode.BOTH_SINGLE_AND_NAMESPACE})
        proc.execute()

        assert proc.outputs is not None
        assert proc.state_label == ProcessState.FINISHED
        assert proc.is_successful
        assert proc.outputs['required_bool'] == False
        assert proc.outputs[namespace]['nested']['one'] == 1
        assert proc.outputs[namespace]['nested']['two'] == 2


class TestProcessEvents:
    def test_basic_events(self):
        proc = utils.DummyProcessWithOutput()
        events_tester = utils.ProcessListenerTester(
            process=proc, expected_events=('running', 'output_emitted', 'finished')
        )
        proc.execute()
        assert events_tester.called == events_tester.expected_events

    def test_killed(self):
        proc = utils.DummyProcessWithOutput()
        events_tester = utils.ProcessListenerTester(proc, ('killed',))
        assert proc.kill()

        # Do the checks
        assert proc.killed()
        assert events_tester.called == events_tester.expected_events

    def test_excepted(self):
        proc = utils.ExceptionProcess()
        events_tester = utils.ProcessListenerTester(
            proc,
            (
                'excepted',
                'running',
                'output_emitted',
            ),
        )
        with pytest.raises(RuntimeError):
            proc.execute()
            proc.result()

        # Do the checks
        assert proc.exception() is not None
        assert events_tester.called == events_tester.expected_events

    def test_paused(self):
        proc = utils.DummyProcessWithOutput()
        events_tester = utils.ProcessListenerTester(proc, ('paused',))
        assert proc.pause()

        # Do the checks
        assert events_tester.called == events_tester.expected_events

    def test_broadcast(self):
        coordinator = utils.MockCoordinator()

        messages = []

        def on_broadcast_receive(_comm, body, sender, subject, correlation_id):
            messages.append({'body': body, 'subject': subject, 'sender': sender, 'correlation_id': correlation_id})

        coordinator.add_broadcast_subscriber(on_broadcast_receive)
        proc = utils.DummyProcess(coordinator=coordinator)
        proc.execute()

        expected_subjects = []
        for i, state in enumerate(utils.DummyProcess.EXPECTED_STATE_SEQUENCE):
            from_state = utils.DummyProcess.EXPECTED_STATE_SEQUENCE[i - 1].value if i != 0 else None
            expected_subjects.append(f'state_changed.{from_state}.{state.value}')

        assert [msg['subject'] for msg in messages] == expected_subjects


class _RestartProcess(utils.WaitForSignalProcess):
    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.outputs.dynamic = True

    def last_step(self):
        self.out('finished', True)
