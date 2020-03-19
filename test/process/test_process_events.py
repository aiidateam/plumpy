# -*- coding: utf-8 -*-
"""Savable Process tests"""
import pytest

import kiwipy

from .. import utils


@pytest.mark.asyncio
async def test_basic_events():
    proc = utils.DummyProcessWithOutput()
    events_tester = utils.ProcessListenerTester(process=proc, expected_events=('running', 'output_emitted', 'finished'))
    await proc.step_until_terminated()
    assert events_tester.called == events_tester.expected_events


def test_killed():
    proc = utils.DummyProcessWithOutput()
    events_tester = utils.ProcessListenerTester(proc, ('killed',))
    assert proc.kill()

    # Do the checks
    assert proc.killed()
    assert events_tester.called == events_tester.expected_events


@pytest.mark.asyncio
async def test_excepted():
    proc = utils.ExceptionProcess()
    events_tester = utils.ProcessListenerTester(proc, (
        'excepted',
        'running',
        'output_emitted',
    ))
    with pytest.raises(RuntimeError):
        await proc.step_until_terminated()
        proc.result()

    # Do the checks
    assert proc.exception() is not None
    assert events_tester.called == events_tester.expected_events


def test_paused():
    proc = utils.DummyProcessWithOutput()
    events_tester = utils.ProcessListenerTester(proc, ('paused',))
    assert proc.pause()

    # Do the checks
    assert events_tester.called == events_tester.expected_events


@pytest.mark.asyncio
async def test_broadcast():
    communicator = kiwipy.LocalCommunicator()

    messages = []

    def on_broadcast_receive(_comm, body, sender, subject, correlation_id):
        messages.append({'body': body, 'subject': subject, 'sender': sender, 'correlation_id': correlation_id})

    communicator.add_broadcast_subscriber(on_broadcast_receive)
    proc = utils.DummyProcess(communicator=communicator)
    await proc.step_until_terminated()

    expected_subjects = []
    for i, state in enumerate(utils.DummyProcess.EXPECTED_STATE_SEQUENCE):
        from_state = utils.DummyProcess.EXPECTED_STATE_SEQUENCE[i - 1].value if i != 0 else None
        expected_subjects.append('state_changed.{}.{}'.format(from_state, state.value))

    for i, message in enumerate(messages):
        assert message['subject'] == expected_subjects[i]
