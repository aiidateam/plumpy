# -*- coding: utf-8 -*-
"""Process and process namespace tests"""
import unittest
import asyncio
import enum
import pytest

import plumpy
from plumpy import Process, ProcessState
from plumpy.utils import AttributesFrozendict

from .. import utils


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

    def on_except(self, exc_info):
        if self.forget_on != 'except':
            super(ForgetToCallParent, self).on_except(exc_info)

    def on_finish(self, result, successful):
        if self.forget_on != 'finish':
            super(ForgetToCallParent, self).on_finish(result, successful)

    def on_kill(self, msg):
        if self.forget_on != 'kill':
            super(ForgetToCallParent, self).on_kill(msg)


def test_spec():
    """
    Check that the references to specs are doing the right thing...
    """
    proc = utils.DummyProcess()
    assert Process.spec() is not utils.DummyProcess.spec()
    assert proc.spec() is utils.DummyProcess.spec()

    class Proc(utils.DummyProcess):  # pylint: disable=too-few-public-methods
        pass

    assert Proc.spec() is not Process.spec()
    assert Proc.spec() is not utils.DummyProcess.spec()
    proc = Proc()
    assert proc.spec() is Proc.spec()


def test_dynamic_inputs():

    class NoDynamic(Process):
        pass

    class WithDynamic(Process):

        @classmethod
        def define(cls, spec):
            super(WithDynamic, cls).define(spec)
            spec.inputs.dynamic = True

    with pytest.raises(ValueError):
        NoDynamic(inputs={'a': 5}).execute()

    proc = WithDynamic(inputs={'a': 5})
    proc.execute()


def test_inputs():

    class Proc(Process):

        @classmethod
        def define(cls, spec):
            super(Proc, cls).define(spec)
            spec.input('a')

    proc = Proc({'a': 5})

    # Check that we can access the inputs after creating
    assert proc.raw_inputs.a == 5
    with pytest.raises(AttributeError):
        proc.raw_inputs.b  # pylint: disable=pointless-statement


def test_inputs_default():

    class Proc(utils.DummyProcess):

        @classmethod
        def define(cls, spec):
            super(Proc, cls).define(spec)
            spec.input('input', default=5, required=False)

    # Supply a value
    proc = Proc(inputs={'input': 2})
    assert proc.inputs['input'] == 2

    # Don't supply, use default
    proc = Proc()
    assert proc.inputs['input'] == 5


def test_inputs_default_that_evaluate_to_false():  #pylint: disable=invalid-name
    for def_val in (True, False, 0, 1):

        class Proc(utils.DummyProcess):

            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)
                spec.input('input', default=def_val)  # pylint: disable=cell-var-from-loop

        # Don't supply, use default
        proc = Proc()
        assert 'input' in proc.inputs
        assert proc.inputs['input'] == def_val


def test_nested_namespace_defaults():
    """Process with a default in a nested namespace should be created, even if top level namespace not supplied."""

    class SomeProcess(Process):

        @classmethod
        def define(cls, spec):
            super(SomeProcess, cls).define(spec)
            spec.input_namespace('namespace', required=False)
            spec.input('namespace.sub', default=True)

    process = SomeProcess()
    assert 'sub' in process.inputs.namespace
    assert process.inputs.namespace.sub


def test_raise_in_define():
    """Process which raises in its 'define' method. Check that the spec is not set."""

    class BrokenProcess(Process):

        @classmethod
        def define(cls, spec):
            super(BrokenProcess, cls).define(spec)
            raise ValueError

    with pytest.raises(ValueError):
        BrokenProcess.spec()
    # Check that the error is still raised when calling .spec()
    # a second time.
    with pytest.raises(ValueError):
        BrokenProcess.spec()


def test_execute():
    proc = utils.DummyProcessWithOutput()
    proc.execute()

    assert proc.done()
    assert proc.state == ProcessState.FINISHED
    assert proc.outputs == {'default': 5}


def test_run_from_class():
    # Test running through class method
    proc = utils.DummyProcessWithOutput()
    proc.execute()
    results = proc.outputs
    assert results['default'] == 5


def test_forget_to_call_parent():
    for event in ('create', 'run', 'finish'):
        with pytest.raises(AssertionError):
            proc = ForgetToCallParent(event)
            proc.execute()


def test_forget_to_call_parent_kill():
    with pytest.raises(AssertionError):
        proc = ForgetToCallParent('kill')
        proc.kill()
        proc.execute()


def test_pid():
    # Test auto generation of pid
    process = utils.DummyProcessWithOutput()
    assert process.pid is not None

    # Test using integer as pid
    process = utils.DummyProcessWithOutput(pid=5)
    assert process.pid == 5

    # Test using string as pid
    process = utils.DummyProcessWithOutput(pid='a')
    assert process.pid == 'a'


def test_exception():
    proc = utils.ExceptionProcess()
    with pytest.raises(RuntimeError):
        proc.execute()
    assert proc.state == ProcessState.EXCEPTED


def test_get_description():

    class ProcWithoutSpec(Process):
        pass

    class ProcWithSpec(Process):
        """ Process with a spec and a docstring """

        @classmethod
        def define(cls, spec):
            super(ProcWithSpec, cls).define(spec)
            spec.input('a', default=1)

    for proc_class in utils.TEST_PROCESSES:
        desc = proc_class.get_description()
        assert isinstance(desc, dict)

    desc_with_spec = ProcWithSpec.get_description()
    desc_without_spec = ProcWithoutSpec.get_description()

    assert isinstance(desc_with_spec, dict)
    assert 'spec' in desc_with_spec
    assert 'description' not in desc_without_spec
    assert isinstance(desc_with_spec['spec'], dict)

    assert isinstance(desc_with_spec, dict)
    assert 'spec' in desc_with_spec
    assert 'description' in desc_with_spec
    assert isinstance(desc_with_spec['spec'], dict)
    assert isinstance(desc_with_spec['description'], str)


def test_logging():

    class LoggerTester(Process):

        def run(self):
            self.logger.info('Test')

    # TODO: Test giving a custom logger to see if it gets used
    proc = LoggerTester()
    proc.execute()


def test_kill():
    proc = utils.DummyProcess()

    proc.kill('Farewell!')
    assert proc.killed()
    assert proc.killed_msg() == 'Farewell!'
    assert proc.state == ProcessState.KILLED


@pytest.mark.asyncio
async def test_wait_continue():
    proc = utils.WaitForSignalProcess()
    # Wait - Execute the process and wait until it is waiting

    listener = plumpy.ProcessListener()
    listener.on_process_waiting = lambda proc: proc.resume()

    proc.add_process_listener(listener)
    await proc.step_until_terminated()

    # Check it's done
    assert proc.done()
    assert proc.state == ProcessState.FINISHED


def test_exc_info():
    proc = utils.ExceptionProcess()
    with pytest.raises(RuntimeError) as excinfo:
        proc.execute()
    assert proc.exception() == excinfo.value


def test_run_done():
    proc = utils.DummyProcess()
    proc.execute()
    assert proc.done()


@pytest.mark.asyncio
async def test_wait_pause_play_resume():
    """
    Test that if you pause a process that and its awaitable finishes that it
    completes correctly when played again.
    """
    proc = utils.WaitForSignalProcess()
    asyncio.ensure_future(proc.step_until_terminated())

    await utils.run_until_waiting(proc)
    assert proc.state == ProcessState.WAITING

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
    assert proc.done()
    assert proc.state == ProcessState.FINISHED


@pytest.mark.asyncio
async def test_pause_play_status_messaging():
    """
    Test the setting of a processes' status through pause and play works correctly.

    Any process can have its status set to a given message. When pausing, a pause message can be set for the
    status, which should store the current status, which should be restored, once the process is played again.
    """
    # pylint: disable=invalid-name
    PLAY_STATUS = 'process was played by Hans Klok'
    PAUSE_STATUS = 'process was paused by Evel Knievel'

    proc = utils.WaitForSignalProcess()
    proc.set_status(PLAY_STATUS)
    asyncio.ensure_future(proc.step_until_terminated())

    await utils.run_until_waiting(proc)
    assert proc.state == ProcessState.WAITING

    result = await proc.pause(PAUSE_STATUS)
    assert result
    assert proc.paused
    assert proc.status == PAUSE_STATUS

    result = proc.play()
    assert proc.status == PLAY_STATUS
    assert proc._pre_paused_status is None  # pylint: disable=protected-access

    proc.resume()
    # Wait until the process is terminated
    await proc.future()

    # Check it's done
    assert proc.done()
    assert proc.state == ProcessState.FINISHED


def test_kill_in_run():

    class KillProcess(Process):
        after_kill = False

        def run(self):
            self.kill()
            # The following line should be executed because kill will not
            # interrupt execution of a method call in the RUNNING state
            self.after_kill = True

    proc = KillProcess()
    with pytest.raises(plumpy.KilledError):
        proc.execute()

    assert proc.after_kill
    assert proc.state == ProcessState.KILLED


def test_kill_when_paused_in_run():

    class PauseProcess(Process):

        def run(self):
            self.pause()
            self.kill()

    proc = PauseProcess()
    with pytest.raises(plumpy.KilledError):
        proc.execute()

    assert proc.state == ProcessState.KILLED


@pytest.mark.asyncio
async def test_kill_when_paused():
    proc = utils.WaitForSignalProcess()

    asyncio.ensure_future(proc.step_until_terminated())
    await utils.run_until_waiting(proc)

    result = await proc.pause()
    assert result
    assert proc.paused

    # Kill the process
    proc.kill()

    with pytest.raises(plumpy.KilledError):
        result = await proc.future()

    assert proc.state == ProcessState.KILLED


@pytest.mark.asyncio
async def test_run_multiple():
    # Create and play some processes
    loop = asyncio.get_event_loop()

    procs = []
    for proc_class in utils.TEST_PROCESSES:
        proc = proc_class(loop=loop)
        procs.append(proc)

    await asyncio.gather(*[p.step_until_terminated() for p in procs])
    futures = await asyncio.gather(*[p.future() for p in procs])

    for future, proc_class in zip(futures, utils.TEST_PROCESSES):
        assert proc_class.EXPECTED_OUTPUTS == future


def test_invalid_output():

    class InvalidOutput(plumpy.Process):

        def run(self):
            self.out('invalid', 5)

    proc = InvalidOutput()
    with pytest.raises(ValueError):
        proc.execute()


def test_missing_output():
    proc = utils.MissingOutputProcess()

    with pytest.raises(plumpy.InvalidStateError):
        proc.successful()

    proc.execute()

    assert not proc.successful()


def test_unsuccessful_result():
    ERROR_CODE = 256  # pylint: disable=invalid-name

    class Proc(Process):

        @classmethod
        def define(cls, spec):
            super(Proc, cls).define(spec)

        def run(self):
            return plumpy.UnsuccessfulResult(ERROR_CODE)

    proc = Proc()
    proc.execute()

    assert proc.result() == ERROR_CODE


def test_pause_in_process():
    """ Test that we can pause and cancel that by playing within the process """

    ioloop = asyncio.get_event_loop()

    class TestPausePlay(plumpy.Process):

        def run(self):
            fut = self.pause()
            assert isinstance(fut, plumpy.Future)

    listener = plumpy.ProcessListener()
    listener.on_process_paused = lambda _proc: ioloop.stop()

    proc = TestPausePlay()
    proc.add_process_listener(listener)

    asyncio.ensure_future(proc.step_until_terminated())
    ioloop.run_forever()
    assert proc.paused
    assert proc.state == ProcessState.FINISHED


@pytest.mark.asyncio
async def test_pause_play_in_process():
    """ Test that we can pause and play that by playing within the process """

    class TestPausePlay(plumpy.Process):

        def run(self):
            fut = self.pause()
            assert isinstance(fut, plumpy.Future)
            result = self.play()
            assert result

    proc = TestPausePlay()

    # asyncio.ensure_future(proc.step_until_terminated())
    await proc.step_until_terminated()
    assert not proc.paused
    assert proc.state == ProcessState.FINISHED


def test_process_stack():

    class StackTest(plumpy.Process):

        def run(self):
            assert self is Process.current()

    proc = StackTest()
    proc.execute()


@pytest.mark.asyncio
async def test_process_stack_multiple():
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
            proc = StackTest()
            asyncio.ensure_future(proc.step_until_terminated())

    to_run = []
    for _ in range(100):
        to_run.append(ParentProcess().step_until_terminated())

    await asyncio.gather(*to_run)

    for res in expect_true:
        assert res


def test_call_soon():

    class CallSoon(plumpy.Process):

        def run(self):
            self.call_soon(self.do_except)

        @staticmethod
        def do_except():
            raise RuntimeError('Breaking yo!')

    # TODO: the function is not correctly tested
    # the expected behaviour of call_soon?
    CallSoon().execute()


def test_execute_twice():
    """Test a process that is executed once finished raises a ClosedError"""
    proc = utils.DummyProcess()
    proc.execute()
    with pytest.raises(plumpy.ClosedError):
        proc.execute()


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


class TestProcessNamespace(unittest.TestCase):

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
                spec.input('label', valid_type=str, required=False)
                spec.input('description', valid_type=str, required=False)
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
        proc = DummyDynamicProcess(inputs=inputs)

        for label, value in proc.inputs['name']['space'].items():
            self.assertTrue(label in inputs['name']['space'])
            self.assertEqual(int(label), value)
            original_inputs.remove(value)

        # Make sure there are no other inputs
        self.assertFalse(original_inputs)

    def test_namespaced_process_outputs(self):
        """Test the output namespacing and validation."""
        namespace = 'integer.namespace'

        class OutputMode(enum.Enum):

            NONE = 0
            DYNAMIC_PORT_NAMESPACE = 1
            SINGLE_REQUIRED_PORT = 2
            BOTH_SINGLE_AND_NAMESPACE = 3

        class DummyDynamicProcess(Process):

            @classmethod
            def define(cls, spec):
                super(DummyDynamicProcess, cls).define(spec)
                spec.input('output_mode', valid_type=OutputMode, default=OutputMode.NONE)
                spec.output('required_bool', valid_type=bool)
                spec.output_namespace(namespace, valid_type=int, dynamic=True)

            def run(self):
                if self.inputs.output_mode == OutputMode.NONE:
                    pass
                elif self.inputs.output_mode == OutputMode.DYNAMIC_PORT_NAMESPACE:
                    self.out(namespace + '.one', 1)
                    self.out(namespace + '.two', 2)
                elif self.inputs.output_mode == OutputMode.SINGLE_REQUIRED_PORT:
                    self.out('required_bool', False)
                elif self.inputs.output_mode == OutputMode.BOTH_SINGLE_AND_NAMESPACE:
                    self.out('required_bool', False)
                    self.out(namespace + '.one', 1)
                    self.out(namespace + '.two', 2)

        # Run the process in default mode which should not add any outputs and therefore fail
        process = DummyDynamicProcess()
        process.execute()

        self.assertEqual(process.state, ProcessState.FINISHED)
        self.assertFalse(process.is_successful)
        self.assertDictEqual(process.outputs, {})

        # Attaching only namespaced ports should fail, because the required port is not added
        process = DummyDynamicProcess(inputs={'output_mode': OutputMode.DYNAMIC_PORT_NAMESPACE})
        process.execute()

        self.assertEqual(process.state, ProcessState.FINISHED)
        self.assertFalse(process.is_successful)
        self.assertEqual(process.outputs['integer']['namespace']['one'], 1)
        self.assertEqual(process.outputs['integer']['namespace']['two'], 2)

        # Attaching only the single required top-level port should be fine
        process = DummyDynamicProcess(inputs={'output_mode': OutputMode.SINGLE_REQUIRED_PORT})
        process.execute()

        self.assertEqual(process.state, ProcessState.FINISHED)
        self.assertTrue(process.is_successful)
        self.assertEqual(process.outputs['required_bool'], False)

        # Attaching both the required and namespaced ports should result in a successful termination
        process = DummyDynamicProcess(inputs={'output_mode': OutputMode.BOTH_SINGLE_AND_NAMESPACE})
        process.execute()

        self.assertIsNotNone(process.outputs)
        self.assertEqual(process.state, ProcessState.FINISHED)
        self.assertTrue(process.is_successful)
        self.assertEqual(process.outputs['required_bool'], False)
        self.assertEqual(process.outputs['integer']['namespace']['one'], 1)
        self.assertEqual(process.outputs['integer']['namespace']['two'], 2)
