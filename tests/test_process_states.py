# -*- coding: utf-8 -*-
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


def test_create_savable(proc: DummyProcess):
    state = Created(proc, run_fn=proc.run)
    assert isinstance(state, Savable)


def test_running_savable(proc: DummyProcess):
    state = Running(proc, run_fn=proc.run)
    assert isinstance(state, Savable)

    ctx = LoadSaveContext(process=proc)
    saved_state = state.save()
    loaded_state = load(saved_state=saved_state, load_context=ctx)
    saved_state2 = loaded_state.save()

    assert saved_state == saved_state2


def test_waiting_savable(proc: DummyProcess):
    state = Waiting(proc, done_callback=proc.run)
    assert isinstance(state, Savable)

    ctx = LoadSaveContext(process=proc)
    saved_state = state.save()
    loaded_state = load(saved_state=saved_state, load_context=ctx)
    saved_state2 = loaded_state.save()

    assert saved_state == saved_state2


def test_excepted_savable():
    state = Excepted(exception=ValueError('dummy'))
    assert isinstance(state, Savable)

    saved_state = state.save()
    loaded_state = load(saved_state=saved_state)
    saved_state2 = loaded_state.save()

    assert saved_state == saved_state2


def test_finished_savable():
    state = Finished(result='done', successful=True)
    assert isinstance(state, Savable)

    saved_state = state.save()
    loaded_state = load(saved_state=saved_state)
    saved_state2 = loaded_state.save()

    assert saved_state == saved_state2


def test_killed_savable():
    state = Killed(msg=MessageBuilder.kill('kill it'))
    assert isinstance(state, Savable)

    saved_state = state.save()
    loaded_state = load(saved_state=saved_state)
    saved_state2 = loaded_state.save()

    assert saved_state == saved_state2


class DummyCmd(Command):
    pass


def test_subclass_command_savable():
    cmd = DummyCmd()
    assert isinstance(cmd, Savable)

    saved = cmd.save()
    loaded = load(saved_state=saved)
    saved2 = loaded.save()

    assert saved == saved2


# FIXME: using pickle loader this should be able to be solved
@pytest.mark.xfail(reason='the default loader can only load obj from python path')
def test_subclass_command_savable_xfail():
    class DummyCmdXfail(Command):
        pass

    cmd = DummyCmdXfail()
    assert isinstance(cmd, Savable)

    saved = cmd.save()
    loaded = load(saved_state=saved)
    saved2 = loaded.save()

    assert saved == saved2
