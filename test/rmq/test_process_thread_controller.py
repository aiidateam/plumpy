import asyncio
import pytest

import kiwipy.rmq
from plumpy import ProcessState

from .. import utils


@pytest.mark.asyncio
async def test_pause(communicator, controller_thread):
    proc = utils.WaitForSignalProcess(communicator=communicator)
    # Send a pause message
    pause_future = await controller_thread.pause_process(proc.pid)
    assert isinstance(pause_future, kiwipy.Future)
    result = await pause_future
    assert isinstance(result, bool)

    # Check that it all went well
    assert result
    assert proc.paused


@pytest.mark.asyncio
async def test_pause_all(communicator, controller_thread):
    """Test pausing all processes on a communicator"""
    procs = []
    for _ in range(10):
        procs.append(utils.WaitForSignalProcess(communicator=communicator))

    controller_thread.pause_all("Slow yo' roll")
    # Wait until they are all paused
    await utils.wait_util(lambda: all([proc.paused for proc in procs]))


@pytest.mark.asyncio
async def test_play_all(communicator, controller_thread):
    """Test pausing all processes on a communicator"""
    procs = []
    for _ in range(10):
        proc = utils.WaitForSignalProcess(communicator=communicator)
        procs.append(proc)
        proc.pause('hold tight')

    assert all([proc.paused for proc in procs])
    controller_thread.play_all()
    # Wait until they are all paused
    await utils.wait_util(lambda: all([not proc.paused for proc in procs]))


@pytest.mark.asyncio
async def test_play(communicator, controller_thread):
    proc = utils.WaitForSignalProcess(communicator=communicator)
    assert proc.pause()

    # Send a play message
    play_future = controller_thread.play_process(proc.pid)
    # Allow the process to respond to the request
    result = await play_future

    # Check that all is as we expect
    assert result
    assert proc.state == ProcessState.CREATED


@pytest.mark.asyncio
async def test_kill(communicator, controller_thread):
    proc = utils.WaitForSignalProcess(communicator=communicator)

    # Send a kill message
    kill_future = await controller_thread.kill_process(proc.pid)
    # Allow the process to respond to the request
    result = await kill_future

    # Check the outcome
    assert result
    # Occasionally fail
    assert proc.state == ProcessState.KILLED


@pytest.mark.asyncio
async def test_kill_all(communicator, controller_thread):
    """Test pausing all processes on a communicator"""
    procs = []
    for _ in range(10):
        procs.append(utils.WaitForSignalProcess(communicator=communicator))

    controller_thread.kill_all('bang bang, I shot you down')
    await utils.wait_util(lambda: all([proc.killed() for proc in procs]))
    assert all([proc.state == ProcessState.KILLED for proc in procs])


@pytest.mark.asyncio
async def test_status(communicator, controller_thread):
    proc = utils.WaitForSignalProcess(communicator=communicator)
    # Run the process in the background
    asyncio.ensure_future(proc.step_until_terminated())

    # Send a status message
    status_future = controller_thread.get_status(proc.pid)
    # Let the process respond
    status = await status_future

    assert status is not None


def test_launch():
    pass
