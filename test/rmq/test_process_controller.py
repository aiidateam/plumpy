# -*- coding: utf-8 -*-
import asyncio
import pytest

from plumpy import ProcessState

from .. import utils


@pytest.mark.asyncio
async def test_pause(communicator, controller):
    proc = utils.WaitForSignalProcess(communicator=communicator)
    # Run the process in the background
    asyncio.ensure_future(proc.step_until_terminated())
    # Send a pause message
    result = await controller.pause_process(proc.pid)

    # Check that it all went well
    assert result
    assert proc.paused


@pytest.mark.asyncio
async def test_play(communicator, controller):
    proc = utils.WaitForSignalProcess(communicator=communicator)
    # Run the process in the background
    asyncio.ensure_future(proc.step_until_terminated())
    assert proc.pause()

    # Send a play message
    result = await controller.play_process(proc.pid)

    # Check that all is as we expect
    assert result
    assert proc.state == ProcessState.WAITING


@pytest.mark.asyncio
async def test_kill(communicator, controller):
    proc = utils.WaitForSignalProcess(communicator=communicator)
    # Run the process in the event loop
    asyncio.ensure_future(proc.step_until_terminated())

    # Send a kill message and wait for it to be done
    result = await controller.kill_process(proc.pid)

    # Check the outcome
    assert result
    assert proc.state == ProcessState.KILLED


@pytest.mark.asyncio
async def test_status(communicator, controller):
    proc = utils.WaitForSignalProcess(communicator=communicator)
    # Run the process in the background
    asyncio.ensure_future(proc.step_until_terminated())

    # Send a status message
    status = await controller.get_status(proc.pid)

    assert status is not None


def test_broadcast(communicator):
    messages = []

    def on_broadcast_receive(**msg):
        messages.append(msg)

    communicator.add_broadcast_subscriber(on_broadcast_receive)
    proc = utils.DummyProcess(communicator=communicator)
    proc.execute()

    expected_subjects = []
    for i, state in enumerate(utils.DummyProcess.EXPECTED_STATE_SEQUENCE):
        from_state = utils.DummyProcess.EXPECTED_STATE_SEQUENCE[i - 1].value if i != 0 else None
        expected_subjects.append('state_changed.{}.{}'.format(from_state, state.value))

    for i, message in enumerate(messages):
        assert message['subject'] == expected_subjects[i]
