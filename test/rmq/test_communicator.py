# -*- coding: utf-8 -*-
from __future__ import absolute_import
from functools import partial
import shutil
import tempfile
import unittest
import uuid
import asyncio
import shortuuid

import pytest
from kiwipy import rmq
from six.moves import range

import plumpy
from plumpy import communications, process_comms
from .. import utils

try:
    import aio_pika
except ImportError:
    aio_pika = None

# pylint: disable=missing-docstring


@pytest.fixture
def persister():
    _tmppath = tempfile.mkdtemp()
    persister = plumpy.PicklePersister(_tmppath)

    yield persister

    shutil.rmtree(_tmppath)


@pytest.fixture
def loop_communicator(persister):
    message_exchange = '{}.{}'.format(__file__, shortuuid.uuid())
    task_exchange = '{}.{}'.format(__file__, shortuuid.uuid())
    task_queue = '{}.{}'.format(__file__, shortuuid.uuid())

    thread_communicator = rmq.RmqThreadCommunicator.connect(
        connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
        message_exchange=message_exchange,
        task_exchange=task_exchange,
        task_queue=task_queue,
        testing_mode=True
    )

    loop = asyncio.get_event_loop()

    communicator = communications.LoopCommunicator(thread_communicator, loop=loop)
    communicator.add_task_subscriber(plumpy.ProcessLauncher(loop, persister=persister))

    yield communicator

    thread_communicator.stop()


@pytest.fixture
def async_controller(loop_communicator: communications.LoopCommunicator):
    yield process_comms.RemoteProcessController(loop_communicator)


class TestLoopCommunicator:
    """Make sure the loop communicator is working as expected"""

    @pytest.mark.asyncio
    async def test_broadcast(self, loop_communicator):
        BROADCAST = {'body': 'present', 'sender': 'Martin', 'subject': 'sup', 'correlation_id': 420}
        broadcast_future = plumpy.Future()

        loop = asyncio.get_event_loop()

        def get_broadcast(_comm, body, sender, subject, correlation_id):
            assert loop is asyncio.get_event_loop()

            broadcast_future.set_result({
                'body': body,
                'sender': sender,
                'subject': subject,
                'correlation_id': correlation_id
            })

        loop_communicator.add_broadcast_subscriber(get_broadcast)
        loop_communicator.broadcast_send(**BROADCAST)

        result = await broadcast_future
        assert result == BROADCAST

    @pytest.mark.asyncio
    async def test_rpc(self, loop_communicator):
        MSG = 'rpc this'
        rpc_future = plumpy.Future()

        loop = asyncio.get_event_loop()

        def get_rpc(_comm, msg):
            assert loop is asyncio.get_event_loop()
            rpc_future.set_result(msg)

        loop_communicator.add_rpc_subscriber(get_rpc, 'rpc')
        loop_communicator.rpc_send('rpc', MSG)

        result = await rpc_future
        assert result == MSG

    @pytest.mark.asyncio
    async def test_task(self, loop_communicator):
        TASK = 'task this'
        task_future = plumpy.Future()

        loop = asyncio.get_event_loop()

        def get_task(_comm, msg):
            assert loop is asyncio.get_event_loop()
            task_future.set_result(msg)

        loop_communicator.add_task_subscriber(get_task)
        loop_communicator.task_send(TASK)

        result = await task_future
        assert result == TASK


class TestTaskActions:

    @pytest.mark.asyncio
    async def test_launch(self, loop_communicator, async_controller):
        # Let the process run to the end
        result = await async_controller.launch_process(utils.DummyProcess)
        # Check that we got a result
        assert result == utils.DummyProcess.EXPECTED_OUTPUTS

    @pytest.mark.asyncio
    async def test_launch_nowait(self, loop_communicator, async_controller):
        """ Testing launching but don't wait, just get the pid """
        pid = await async_controller.launch_process(utils.DummyProcess, nowait=True)
        assert isinstance(pid, uuid.UUID)

    @pytest.mark.asyncio
    async def test_execute_action(self, loop_communicator, async_controller):
        """ Test the process execute action """
        result = await async_controller.execute_process(utils.DummyProcessWithOutput)
        assert utils.DummyProcessWithOutput.EXPECTED_OUTPUTS == result

    @pytest.mark.asyncio
    async def test_execute_action_nowait(self, loop_communicator, async_controller):
        """ Test the process execute action """
        pid = await async_controller.execute_process(utils.DummyProcessWithOutput, nowait=True)
        assert isinstance(pid, uuid.UUID)

    @pytest.mark.asyncio
    async def test_launch_many(self, loop_communicator, async_controller):
        """Test launching multiple processes"""
        num_to_launch = 10

        launch_futures = []
        for _ in range(num_to_launch):
            launch = async_controller.launch_process(utils.DummyProcess, nowait=True)
            launch_futures.append(launch)

        results = await asyncio.gather(*launch_futures)
        for result in results:
            assert isinstance(result, uuid.UUID)

    @pytest.mark.asyncio
    async def test_continue(self, async_controller, persister):
        """ Test continuing a saved process """
        process = utils.DummyProcessWithOutput()
        persister.save_checkpoint(process)
        pid = process.pid
        del process

        # Let the process run to the end
        result = await async_controller.continue_process(pid)
        assert result, utils.DummyProcessWithOutput.EXPECTED_OUTPUTS
