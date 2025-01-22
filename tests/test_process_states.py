from typing import Any
import pytest

from plumpy import process_states
from plumpy.base.state_machine import StateMachine
from plumpy.message import MessageBuilder
from plumpy.persistence import LoadSaveContext, Savable, load
from plumpy.process_states import Command, Created, Excepted, Finished, Killed, Running, Waiting
from plumpy.processes import Process


class DummyProcess(Process):
    """
    Process with no inputs or outputs and does nothing when ran.
    """

    EXPECTED_STATE_SEQUENCE = [
        process_states.ProcessState.CREATED,
        process_states.ProcessState.RUNNING,
        process_states.ProcessState.FINISHED,
    ]

    EXPECTED_OUTPUTS = {}

    def run(self) -> Any:
        pass


@pytest.fixture(scope='function')
def proc() -> 'StateMachine':
    return DummyProcess()


def test_create_savable(proc: StateMachine):
    state = Created(proc, run_fn=lambda: None)
    assert isinstance(state, Savable)


def test_running_savable(proc: StateMachine):
    state = Running(proc, run_fn=lambda: None)
    assert isinstance(state, Savable)


def test_waiting_savable(proc: StateMachine):
    state = Waiting(proc, done_callback=lambda: None)
    assert isinstance(state, Savable)


def test_excepted_savable():
    state = Excepted(exception=ValueError('dummy'))
    assert isinstance(state, Savable)


def test_finished_savable():
    state = Finished(result='done', successful=True)
    assert isinstance(state, Savable)


def test_killed_savable():
    state = Killed(msg=MessageBuilder.kill('kill it'))
    assert isinstance(state, Savable)


def test_subclass_command_savable():
    class DummyCmd(Command):
        pass

    assert isinstance(DummyCmd(), Savable)


def test_create_save_load(proc: DummyProcess):
    state = Created(proc, run_fn=proc.run)
    ctx = LoadSaveContext(process=proc)
    saved_state = state.save(ctx)
    loaded_state = load(saved_state=saved_state, load_context=ctx)

    # __import__('ipdb').set_trace()


def test_running_save_load(proc: StateMachine):
    state = Running(proc, run_fn=lambda: None)
    assert isinstance(state, Savable)
