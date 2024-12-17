# -*- coding: utf-8 -*-
"""Tests for the :mod:`plumpy.rmq.communicator` module."""

import asyncio
import functools
import shutil
import tempfile
import uuid
import pytest
import shortuuid
import yaml

import kiwipy
from kiwipy.rmq import RmqThreadCommunicator

import plumpy
from plumpy.coordinator import Coordinator
from plumpy.rmq import communications, process_control

from .. import utils


@pytest.fixture
def persister():
    _tmppath = tempfile.mkdtemp()
    persister = plumpy.PicklePersister(_tmppath)

    yield persister

    shutil.rmtree(_tmppath)


class CoordinatorWithLoopRmqThreadCommunicator:
    def __init__(self):
        message_exchange = f'{__file__}.{shortuuid.uuid()}'
        task_exchange = f'{__file__}.{shortuuid.uuid()}'
        task_queue = f'{__file__}.{shortuuid.uuid()}'

        thread_comm = RmqThreadCommunicator.connect(
            connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
            message_exchange=message_exchange,
            task_exchange=task_exchange,
            task_queue=task_queue,
            decoder=functools.partial(yaml.load, Loader=yaml.Loader),
        )

        loop = asyncio.get_event_loop()
        loop.set_debug(True)
        self._comm = communications.LoopCommunicator(thread_comm, loop=loop)

    def add_rpc_subscriber(self, subscriber, identifier=None):
        return self._comm.add_rpc_subscriber(subscriber, identifier)

    def add_broadcast_subscriber(
        self,
        subscriber,
        subject_filter=None,
        identifier=None,
    ):
        subscriber = kiwipy.BroadcastFilter(subscriber, subject=subject_filter)
        return self._comm.add_broadcast_subscriber(subscriber, identifier)

    def add_task_subscriber(self, subscriber, identifier=None):
        return self._comm.add_task_subscriber(subscriber, identifier)

    def remove_rpc_subscriber(self, identifier):
        return self._comm.remove_rpc_subscriber(identifier)

    def remove_broadcast_subscriber(self, identifier):
        return self._comm.remove_broadcast_subscriber(identifier)

    def remove_task_subscriber(self, identifier):
        return self._comm.remove_task_subscriber(identifier)

    def rpc_send(self, recipient_id, msg):
        return self._comm.rpc_send(recipient_id, msg)

    def broadcast_send(
        self,
        body,
        sender=None,
        subject=None,
        correlation_id=None,
    ):
        return self._comm.broadcast_send(body, sender, subject, correlation_id)

    def task_send(self, task, no_reply=False):
        return self._comm.task_send(task, no_reply)

    def close(self):
        self._comm.close()


@pytest.fixture
def _coordinator():
    coordinator = CoordinatorWithLoopRmqThreadCommunicator()
    yield coordinator
    coordinator.close()


@pytest.fixture
def async_controller(_coordinator):
    yield process_control.RemoteProcessController(_coordinator)


class TestLoopCommunicator:
    """Make sure the loop communicator is working as expected"""

    @pytest.mark.asyncio
    async def test_broadcast(self, _coordinator):
        BROADCAST = {'body': 'present', 'sender': 'Martin', 'subject': 'sup', 'correlation_id': 420}  # noqa: N806
        broadcast_future = asyncio.Future()

        loop = asyncio.get_event_loop()

        def get_broadcast(_comm, body, sender, subject, correlation_id):
            assert loop is asyncio.get_event_loop()

            broadcast_future.set_result(
                {'body': body, 'sender': sender, 'subject': subject, 'correlation_id': correlation_id}
            )

        _coordinator.add_broadcast_subscriber(get_broadcast)
        _coordinator.broadcast_send(**BROADCAST)

        result = await broadcast_future
        assert result == BROADCAST

    @pytest.mark.asyncio
    async def test_broadcast_filter(self, _coordinator: Coordinator):
        broadcast_future = asyncio.Future()

        def ignore_broadcast(_comm, body, sender, subject, correlation_id):
            broadcast_future.set_exception(AssertionError('broadcast received'))

        def get_broadcast(_comm, body, sender, subject, correlation_id):
            broadcast_future.set_result(True)

        _coordinator.add_broadcast_subscriber(ignore_broadcast, subject_filter='other')
        _coordinator.add_broadcast_subscriber(get_broadcast)
        _coordinator.broadcast_send(**{'body': 'present', 'sender': 'Martin', 'subject': 'sup', 'correlation_id': 420})

        result = await broadcast_future
        assert result is True

    @pytest.mark.asyncio
    async def test_rpc(self, _coordinator):
        MSG = 'rpc this'  # noqa: N806
        rpc_future = asyncio.Future()

        loop = asyncio.get_event_loop()

        def get_rpc(_comm, msg):
            assert loop is asyncio.get_event_loop()
            rpc_future.set_result(msg)

        _coordinator.add_rpc_subscriber(get_rpc, 'rpc')
        _coordinator.rpc_send('rpc', MSG)

        result = await rpc_future
        assert result == MSG

    @pytest.mark.asyncio
    async def test_task(self, _coordinator):
        TASK = 'task this'  # noqa: N806
        task_future = asyncio.Future()

        loop = asyncio.get_event_loop()

        def get_task(_comm, msg):
            assert loop is asyncio.get_event_loop()
            task_future.set_result(msg)

        _coordinator.add_task_subscriber(get_task)
        _coordinator.task_send(TASK)

        result = await task_future
        assert result == TASK


class TestTaskActions:
    @pytest.mark.asyncio
    async def test_launch(self, _coordinator, async_controller, persister):
        # Let the process run to the end
        loop = asyncio.get_event_loop()
        _coordinator.add_task_subscriber(plumpy.ProcessLauncher(loop, persister=persister))
        result = await async_controller.launch_process(utils.DummyProcess)
        # Check that we got a result
        assert result == utils.DummyProcess.EXPECTED_OUTPUTS

    @pytest.mark.asyncio
    async def test_launch_nowait(self, _coordinator, async_controller, persister):
        """Testing launching but don't wait, just get the pid"""
        loop = asyncio.get_event_loop()
        _coordinator.add_task_subscriber(plumpy.ProcessLauncher(loop, persister=persister))
        pid = await async_controller.launch_process(utils.DummyProcess, nowait=True)
        assert isinstance(pid, uuid.UUID)

    @pytest.mark.asyncio
    async def test_execute_action(self, _coordinator, async_controller, persister):
        """Test the process execute action"""
        loop = asyncio.get_event_loop()
        _coordinator.add_task_subscriber(plumpy.ProcessLauncher(loop, persister=persister))
        result = await async_controller.execute_process(utils.DummyProcessWithOutput)
        assert utils.DummyProcessWithOutput.EXPECTED_OUTPUTS == result

    @pytest.mark.asyncio
    async def test_execute_action_nowait(self, _coordinator, async_controller, persister):
        """Test the process execute action"""
        loop = asyncio.get_event_loop()
        _coordinator.add_task_subscriber(plumpy.ProcessLauncher(loop, persister=persister))
        pid = await async_controller.execute_process(utils.DummyProcessWithOutput, nowait=True)
        assert isinstance(pid, uuid.UUID)

    @pytest.mark.asyncio
    async def test_launch_many(self, _coordinator, async_controller, persister):
        """Test launching multiple processes"""
        loop = asyncio.get_event_loop()
        _coordinator.add_task_subscriber(plumpy.ProcessLauncher(loop, persister=persister))
        num_to_launch = 10

        launch_futures = []
        for _ in range(num_to_launch):
            launch = async_controller.launch_process(utils.DummyProcess, nowait=True)
            launch_futures.append(launch)

        results = await asyncio.gather(*launch_futures)
        for result in results:
            assert isinstance(result, uuid.UUID)

    @pytest.mark.asyncio
    async def test_continue(self, _coordinator, async_controller, persister):
        """Test continuing a saved process"""
        loop = asyncio.get_event_loop()
        _coordinator.add_task_subscriber(plumpy.ProcessLauncher(loop, persister=persister))
        process = utils.DummyProcessWithOutput()
        persister.save_checkpoint(process)
        pid = process.pid
        del process

        # Let the process run to the end
        result = await async_controller.continue_process(pid)
        assert result, utils.DummyProcessWithOutput.EXPECTED_OUTPUTS
