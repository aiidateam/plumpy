# FIXME: after deabstract on savable into a protocol, test that all state are savable

import pytest
from plumpy.base.state_machine import StateMachine
from plumpy.message import MessageBuilder
from plumpy.persistence import Savable
from plumpy.process_states import Created, Excepted, Finished, Killed, Running, Waiting
from tests.utils import DummyProcess


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
