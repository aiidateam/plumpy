# -*- coding: utf-8 -*-
"""Tests for the :mod:`plumpy.rmq.communicator` module."""

import asyncio
import shutil
import tempfile
import uuid
import pytest
import shortuuid

from kiwipy.rmq import RmqThreadCommunicator

import plumpy
from plumpy.rmq import communications, process_control

from . import RmqCoordinator
from .. import utils


@pytest.fixture
def persister():
    _tmppath = tempfile.mkdtemp()
    persister = plumpy.PicklePersister(_tmppath)

    yield persister

    shutil.rmtree(_tmppath)


@pytest.fixture(scope='function')
def make_coordinator():
    def _coordinator(loop=None):
        message_exchange = f'{__file__}.{shortuuid.uuid()}'
        task_exchange = f'{__file__}.{shortuuid.uuid()}'
        task_queue = f'{__file__}.{shortuuid.uuid()}'

        thread_comm = RmqThreadCommunicator.connect(
            connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
            message_exchange=message_exchange,
            task_exchange=task_exchange,
            task_queue=task_queue,
            encoder=process_control.MSGPACK_ENCODER,
            decoder=process_control.MSGPACK_DECODER,
        )

        loop = loop or asyncio.get_event_loop()
        loop.set_debug(True)
        comm = communications.LoopCommunicator(thread_comm, loop=loop)
        coordinator = RmqCoordinator(comm)

        return coordinator

    return _coordinator


@pytest.fixture(scope='function')
def make_controller(make_coordinator):
    def _controller(loop=None):
        coordinator = make_coordinator(loop)
        controller = process_control.RemoteProcessController(coordinator)

        return controller

    return _controller


class TestLoopCommunicator:
    """Make sure the loop communicator is working as expected"""

    @pytest.mark.asyncio
    async def test_broadcast(self, make_coordinator):
        loop = asyncio.get_running_loop()
        coordinator = make_coordinator(loop)
        BROADCAST = {'body': 'present', 'sender': 'Martin', 'subject': 'sup', 'correlation_id': 420}  # noqa: N806
        broadcast_future = asyncio.Future()

        def get_broadcast(_comm, body, sender, subject, correlation_id):
            assert loop is asyncio.get_running_loop()

            broadcast_future.set_result(
                {'body': body, 'sender': sender, 'subject': subject, 'correlation_id': correlation_id}
            )

        coordinator.add_broadcast_subscriber(get_broadcast)
        coordinator.broadcast_send(**BROADCAST)

        result = await broadcast_future
        assert result == BROADCAST

    @pytest.mark.asyncio
    async def test_broadcast_filter(self, make_coordinator):
        loop = asyncio.get_running_loop()
        coordinator = make_coordinator(loop)

        broadcast_future = asyncio.Future()

        def ignore_broadcast(_comm, body, sender, subject, correlation_id):
            broadcast_future.set_exception(AssertionError('broadcast received'))

        def get_broadcast(_comm, body, sender, subject, correlation_id):
            broadcast_future.set_result(True)

        coordinator.add_broadcast_subscriber(ignore_broadcast, subject_filters=['other'])
        coordinator.add_broadcast_subscriber(get_broadcast)
        coordinator.broadcast_send(**{'body': 'present', 'sender': 'Martin', 'subject': 'sup', 'correlation_id': 420})

        result = await broadcast_future
        assert result is True

    @pytest.mark.asyncio
    async def test_rpc(self, make_coordinator):
        loop = asyncio.get_running_loop()
        coordinator = make_coordinator(loop)

        MSG = 'rpc this'  # noqa: N806
        rpc_future = asyncio.Future()

        loop = asyncio.get_event_loop()

        def get_rpc(_comm, msg):
            assert loop is asyncio.get_event_loop()
            rpc_future.set_result(msg)

        coordinator.add_rpc_subscriber(get_rpc, 'rpc')
        coordinator.rpc_send('rpc', MSG)

        result = await rpc_future
        assert result == MSG

    @pytest.mark.asyncio
    async def test_task(self, make_coordinator):
        loop = asyncio.get_running_loop()
        coordinator = make_coordinator(loop)

        TASK = 'task this'  # noqa: N806
        task_future = asyncio.Future()

        loop = asyncio.get_event_loop()

        def get_task(_comm, msg):
            assert loop is asyncio.get_event_loop()
            task_future.set_result(msg)

        coordinator.add_task_subscriber(get_task)
        coordinator.task_send(TASK)

        result = await task_future
        assert result == TASK


class TestTaskActions:
    @pytest.mark.asyncio
    async def test_launch(self, make_controller, persister):
        # Let the process run to the end
        loop = asyncio.get_running_loop()
        controller = make_controller(loop)
        controller.coordinator.add_task_subscriber(plumpy.ProcessLauncher(loop, persister=persister))
        result = await controller.launch_process(utils.DummyProcess)
        # Check that we got a result
        assert result == utils.DummyProcess.EXPECTED_OUTPUTS

    @pytest.mark.asyncio
    async def test_launch_nowait(self, make_controller, persister):
        """Testing launching but don't wait, just get the pid"""
        loop = asyncio.get_running_loop()
        controller = make_controller(loop)
        controller.coordinator.add_task_subscriber(plumpy.ProcessLauncher(loop, persister=persister))
        pid = await controller.launch_process(utils.DummyProcess, nowait=True)
        assert isinstance(pid, uuid.UUID)

    @pytest.mark.asyncio
    async def test_execute_action(self, make_controller, persister):
        """Test the process execute action"""
        loop = asyncio.get_running_loop()
        controller = make_controller(loop)
        controller.coordinator.add_task_subscriber(plumpy.ProcessLauncher(loop, persister=persister))
        result = await controller.execute_process(utils.DummyProcessWithOutput)
        assert utils.DummyProcessWithOutput.EXPECTED_OUTPUTS == result

    @pytest.mark.asyncio
    async def test_execute_action_nowait(self, make_controller, persister):
        """Test the process execute action"""
        loop = asyncio.get_running_loop()
        controller = make_controller(loop)
        controller.coordinator.add_task_subscriber(plumpy.ProcessLauncher(loop, persister=persister))
        pid = await controller.execute_process(utils.DummyProcessWithOutput, nowait=True)
        assert isinstance(pid, uuid.UUID)

    @pytest.mark.asyncio
    async def test_launch_many(self, make_controller, persister):
        """Test launching multiple processes"""
        loop = asyncio.get_running_loop()
        controller = make_controller(loop)
        controller.coordinator.add_task_subscriber(plumpy.ProcessLauncher(loop, persister=persister))
        num_to_launch = 10

        launch_futures = []
        for _ in range(num_to_launch):
            launch = controller.launch_process(utils.DummyProcess, nowait=True)
            launch_futures.append(launch)

        results = await asyncio.gather(*launch_futures)
        for result in results:
            assert isinstance(result, uuid.UUID)

    @pytest.mark.asyncio
    async def test_continue(self, make_controller, persister):
        """Test continuing a saved process"""
        loop = asyncio.get_running_loop()
        controller = make_controller(loop)
        controller.coordinator.add_task_subscriber(plumpy.ProcessLauncher(loop, persister=persister))
        process = utils.DummyProcessWithOutput()
        persister.save_checkpoint(process)
        pid = process.pid
        del process

        # Let the process run to the end
        result = await controller.continue_process(pid)
        assert result, utils.DummyProcessWithOutput.EXPECTED_OUTPUTS
