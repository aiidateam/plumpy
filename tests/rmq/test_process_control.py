# -*- coding: utf-8 -*-
import asyncio

import kiwipy
import pytest
import shortuuid
from kiwipy import rmq

import plumpy
from plumpy.rmq import process_control

from . import RmqCoordinator
from .. import utils


@pytest.fixture
def _coordinator():
    message_exchange = f'{__file__}.{shortuuid.uuid()}'
    task_exchange = f'{__file__}.{shortuuid.uuid()}'
    task_queue = f'{__file__}.{shortuuid.uuid()}'

    comm = rmq.RmqThreadCommunicator.connect(
        connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
        message_exchange=message_exchange,
        task_exchange=task_exchange,
        task_queue=task_queue,
    )
    comm._loop.set_debug(True)
    coordinator = RmqCoordinator(comm)

    yield coordinator

    coordinator.close()


@pytest.fixture
def async_controller(_coordinator):
    yield process_control.RemoteProcessController(_coordinator)


@pytest.fixture
def sync_controller(_coordinator):
    yield process_control.RemoteProcessThreadController(_coordinator)


class TestRemoteProcessController:
    @pytest.mark.asyncio
    async def test_pause(self, _coordinator, async_controller):
        proc = utils.WaitForSignalProcess(coordinator=_coordinator)
        # Run the process in the background
        asyncio.ensure_future(proc.step_until_terminated())
        # Send a pause message
        result = await async_controller.pause_process(proc.pid)

        # Check that it all went well
        assert result
        assert proc.paused

    @pytest.mark.asyncio
    async def test_play(self, _coordinator, async_controller):
        proc = utils.WaitForSignalProcess(coordinator=_coordinator)
        # Run the process in the background
        asyncio.ensure_future(proc.step_until_terminated())
        assert proc.pause()

        # Send a play message
        result = await async_controller.play_process(proc.pid)

        # Check that all is as we expect
        assert result
        assert proc.state_label == plumpy.ProcessState.WAITING

        # if not close the background process will raise exception
        # make sure proc reach the final state
        await async_controller.kill_process(proc.pid)

    @pytest.mark.asyncio
    async def test_kill(self, _coordinator, async_controller):
        proc = utils.WaitForSignalProcess(coordinator=_coordinator)
        # Run the process in the event loop
        asyncio.ensure_future(proc.step_until_terminated())

        # Send a kill message and wait for it to be done
        result = await async_controller.kill_process(proc.pid)

        # Check the outcome
        assert result
        assert proc.state_label == plumpy.ProcessState.KILLED

    @pytest.mark.asyncio
    async def test_status(self, _coordinator, async_controller):
        proc = utils.WaitForSignalProcess(coordinator=_coordinator)
        # Run the process in the background
        asyncio.ensure_future(proc.step_until_terminated())

        # Send a status message
        status = await async_controller.get_status(proc.pid)

        assert status is not None

        # make sure proc reach the final state
        await async_controller.kill_process(proc.pid)

    def test_broadcast(self, _coordinator):
        messages = []

        def on_broadcast_receive(**msg):
            messages.append(msg)

        _coordinator.add_broadcast_subscriber(on_broadcast_receive)

        proc = utils.DummyProcess(coordinator=_coordinator)
        proc.execute()

        expected_subjects = []
        for i, state in enumerate(utils.DummyProcess.EXPECTED_STATE_SEQUENCE):
            from_state = utils.DummyProcess.EXPECTED_STATE_SEQUENCE[i - 1].value if i != 0 else None
            expected_subjects.append(f'state_changed.{from_state}.{state.value}')

        for i, message in enumerate(messages):
            self.assertEqual(message['subject'], expected_subjects[i])


class TestRemoteProcessThreadController:
    @pytest.mark.asyncio
    async def test_pause(self, _coordinator, sync_controller):
        proc = utils.WaitForSignalProcess(coordinator=_coordinator)

        # Send a pause message
        pause_future = sync_controller.pause_process(proc.pid)
        assert isinstance(pause_future, kiwipy.Future)
        future = await asyncio.wrap_future(pause_future)
        result = future.result()

        # Check that it all went well
        assert result
        assert proc.paused

    @pytest.mark.asyncio
    async def test_pause_all(self, _coordinator, sync_controller):
        """Test pausing all processes on a communicator"""
        procs = []
        for _ in range(10):
            procs.append(utils.WaitForSignalProcess(coordinator=_coordinator))

        sync_controller.pause_all("Slow yo' roll")
        # Wait until they are all paused
        await utils.wait_util(lambda: all([proc.paused for proc in procs]))

    @pytest.mark.asyncio
    async def test_play_all(self, _coordinator, sync_controller):
        """Test pausing all processes on a communicator"""
        procs = []
        for _ in range(10):
            proc = utils.WaitForSignalProcess(coordinator=_coordinator)
            procs.append(proc)
            proc.pause('hold tight')

        assert all([proc.paused for proc in procs])
        sync_controller.play_all()
        # Wait until they are all paused
        await utils.wait_util(lambda: all([not proc.paused for proc in procs]))

    @pytest.mark.asyncio
    async def test_play(self, _coordinator, sync_controller):
        proc = utils.WaitForSignalProcess(coordinator=_coordinator)
        assert proc.pause()

        # Send a play message
        play_future = sync_controller.play_process(proc.pid)
        # Allow the process to respond to the request
        result = await asyncio.wrap_future(play_future)

        # Check that all is as we expect
        assert result
        assert proc.state_label == plumpy.ProcessState.CREATED

    @pytest.mark.asyncio
    async def test_kill(self, _coordinator, sync_controller):
        proc = utils.WaitForSignalProcess(coordinator=_coordinator)

        # Send a kill message
        kill_future = sync_controller.kill_process(proc.pid)
        # Allow the process to respond to the request
        result = await asyncio.wrap_future(kill_future)

        # Check the outcome
        assert result
        # Occasionally fail
        assert proc.state_label == plumpy.ProcessState.KILLED

    @pytest.mark.asyncio
    async def test_kill_all(self, _coordinator, sync_controller):
        """Test pausing all processes on a communicator"""
        procs = []
        for _ in range(10):
            procs.append(utils.WaitForSignalProcess(coordinator=_coordinator))

        sync_controller.kill_all(msg_text='bang bang, I shot you down')
        await utils.wait_util(lambda: all([proc.killed() for proc in procs]))
        assert all([proc.state_label == plumpy.ProcessState.KILLED for proc in procs])

    @pytest.mark.asyncio
    async def test_status(self, _coordinator, sync_controller):
        proc = utils.WaitForSignalProcess(coordinator=_coordinator)
        # Run the process in the background
        asyncio.ensure_future(proc.step_until_terminated())

        # Send a status message
        status_future = sync_controller.get_status(proc.pid)
        # Let the process respond
        status = await asyncio.wrap_future(status_future)

        assert status is not None

        kill_future = sync_controller.kill_process(proc.pid)
        await asyncio.wrap_future(kill_future)
