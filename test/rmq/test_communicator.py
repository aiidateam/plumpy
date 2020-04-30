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
from tornado import testing, ioloop

import plumpy
from plumpy import communications, process_comms
from ..utils import AsyncTestCase
from .. import utils

try:
    import aio_pika
except ImportError:
    aio_pika = None

AWAIT_TIMEOUT = testing.get_async_test_timeout()

# pylint: disable=missing-docstring


class CommunicatorTestCase(unittest.TestCase):

    def setUp(self):
        super(CommunicatorTestCase, self).setUp()
        message_exchange = '{}.{}'.format(self.__class__.__name__, shortuuid.uuid())
        task_exchange = '{}.{}'.format(self.__class__.__name__, shortuuid.uuid())
        queue_name = '{}.{}.tasks'.format(self.__class__.__name__, shortuuid.uuid())

        self.rmq_communicator = rmq.connect(
            connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
            message_exchange=message_exchange,
            task_exchange=task_exchange,
            task_queue=queue_name,
            testing_mode=True
        )
        self.loop = asyncio.get_event_loop()
        self.communicator = communications.LoopCommunicator(self.rmq_communicator, self.loop)

    def tearDown(self):
        # Close the connector before calling super because it will close the loop
        self.rmq_communicator.stop()
        super(CommunicatorTestCase, self).tearDown()


@unittest.skipIf(not aio_pika, 'Requires pika library and RabbitMQ')
class TestLoopCommunicator(CommunicatorTestCase):
    """Make sure the loop communicator is working as expected"""

    @pytest.mark.asyncio
    async def test_broadcast(self):
        BROADCAST = {'body': 'present', 'sender': 'Martin', 'subject': 'sup', 'correlation_id': 420}
        broadcast_future = plumpy.Future()

        def get_broadcast(_comm, body, sender, subject, correlation_id):
            self.assertEqual(self.loop, asyncio.get_event_loop())
            broadcast_future.set_result({
                'body': body,
                'sender': sender,
                'subject': subject,
                'correlation_id': correlation_id
            })

        self.communicator.add_broadcast_subscriber(get_broadcast)
        self.communicator.broadcast_send(**BROADCAST)

        result = await broadcast_future
        self.assertDictEqual(BROADCAST, result)

    @pytest.mark.asyncio
    async def test_rpc(self):
        MSG = 'rpc this'
        rpc_future = plumpy.Future()

        def get_rpc(_comm, msg):
            self.assertEqual(self.loop, asyncio.get_event_loop())
            rpc_future.set_result(msg)

        self.communicator.add_rpc_subscriber(get_rpc, 'rpc')
        self.communicator.rpc_send('rpc', MSG)

        result = await rpc_future
        self.assertEqual(MSG, result)

    @pytest.mark.asyncio
    async def test_task(self):
        TASK = 'task this'
        task_future = plumpy.Future()

        def get_task(_comm, msg):
            self.assertEqual(self.loop, asyncio.get_event_loop())
            task_future.set_result(msg)

        self.communicator.add_task_subscriber(get_task)
        self.communicator.task_send(TASK)

        result = await task_future
        self.assertEqual(TASK, result)


@unittest.skipIf(not aio_pika, 'Requires pika library and RabbitMQ')
class TestTaskActions(CommunicatorTestCase):

    def setUp(self):
        super(TestTaskActions, self).setUp()
        self._tmppath = tempfile.mkdtemp()
        self.persister = plumpy.PicklePersister(self._tmppath)
        # Add the process launcher
        self.communicator.add_task_subscriber(plumpy.ProcessLauncher(self.loop, persister=self.persister))

        self.process_controller = process_comms.RemoteProcessController(self.communicator)

    def tearDown(self):
        # Close the connector before calling super because it will
        super(TestTaskActions, self).tearDown()
        shutil.rmtree(self._tmppath)

    @pytest.mark.asyncio
    async def test_launch(self):
        # Let the process run to the end
        result = await self.process_controller.launch_process(utils.DummyProcess)
        # Check that we got a result
        self.assertDictEqual(utils.DummyProcess.EXPECTED_OUTPUTS, result)

    @pytest.mark.asyncio
    async def test_launch_nowait(self):
        """ Testing launching but don't wait, just get the pid """
        pid = await self.process_controller.launch_process(utils.DummyProcess, nowait=True)
        self.assertIsInstance(pid, uuid.UUID)

    @pytest.mark.asyncio
    async def test_execute_action(self):
        """ Test the process execute action """
        result = await self.process_controller.execute_process(utils.DummyProcessWithOutput)
        self.assertEqual(utils.DummyProcessWithOutput.EXPECTED_OUTPUTS, result)

    @pytest.mark.asyncio
    async def test_execute_action_nowait(self):
        """ Test the process execute action """
        pid = await self.process_controller.execute_process(utils.DummyProcessWithOutput, nowait=True)
        self.assertIsInstance(pid, uuid.UUID)

    @pytest.mark.asyncio
    async def test_launch_many(self):
        """Test launching multiple processes"""
        num_to_launch = 10

        launch_futures = []
        for _ in range(num_to_launch):
            launch = self.process_controller.launch_process(utils.DummyProcess, nowait=True)
            launch_futures.append(launch)

        results = await asyncio.gather(*launch_futures)
        for result in results:
            self.assertIsInstance(result, uuid.UUID)

    @pytest.mark.asyncio
    async def test_continue(self):
        """ Test continuing a saved process """
        process = utils.DummyProcessWithOutput()
        self.persister.save_checkpoint(process)
        pid = process.pid
        del process

        # Let the process run to the end
        result = await self.process_controller.continue_process(pid)
        self.assertEqual(result, utils.DummyProcessWithOutput.EXPECTED_OUTPUTS)
