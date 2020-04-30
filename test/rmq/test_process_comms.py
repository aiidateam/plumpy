# -*- coding: utf-8 -*-
import unittest
import asyncio

import shortuuid
import kiwipy.rmq

import plumpy
import plumpy.communications
from plumpy import process_comms
from .. import utils
import pytest

try:
    import aio_pika
except ImportError:
    aio_pika = None


@unittest.skipIf(not aio_pika, 'Requires pika library and RabbitMQ')
class TestRemoteProcessController(unittest.TestCase):

    def setUp(self):
        super().setUp()

        message_exchange = '{}.{}'.format(self.__class__.__name__, shortuuid.uuid())
        task_exchange = '{}.{}'.format(self.__class__.__name__, shortuuid.uuid())
        task_queue = '{}.{}'.format(self.__class__.__name__, shortuuid.uuid())

        self.communicator = kiwipy.rmq.connect(
            connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
            message_exchange=message_exchange,
            task_exchange=task_exchange,
            task_queue=task_queue,
            testing_mode=True
        )
        self.process_controller = process_comms.RemoteProcessController(self.communicator)

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.communicator.stop()
        super().tearDown()

    @pytest.mark.asyncio
    async def test_pause(self):
        proc = utils.WaitForSignalProcess(communicator=self.communicator)
        # Run the process in the background
        asyncio.ensure_future(proc.step_until_terminated())
        # Send a pause message
        result = await self.process_controller.pause_process(proc.pid)

        # Check that it all went well
        self.assertTrue(result)
        self.assertTrue(proc.paused)

    @pytest.mark.asyncio
    async def test_play(self):
        proc = utils.WaitForSignalProcess(communicator=self.communicator)
        # Run the process in the background
        asyncio.ensure_future(proc.step_until_terminated())
        self.assertTrue(proc.pause())

        # Send a play message
        result = await self.process_controller.play_process(proc.pid)

        # Check that all is as we expect
        self.assertTrue(result)
        self.assertEqual(proc.state, plumpy.ProcessState.WAITING)

    @pytest.mark.asyncio
    async def test_kill(self):
        proc = utils.WaitForSignalProcess(communicator=self.communicator)
        # Run the process in the event loop
        asyncio.ensure_future(proc.step_until_terminated())

        # Send a kill message and wait for it to be done
        result = await self.process_controller.kill_process(proc.pid)

        # Check the outcome
        self.assertTrue(result)
        self.assertEqual(proc.state, plumpy.ProcessState.KILLED)

    @pytest.mark.asyncio
    async def test_status(self):
        proc = utils.WaitForSignalProcess(communicator=self.communicator)
        # Run the process in the background
        asyncio.ensure_future(proc.step_until_terminated())

        # Send a status message
        status = await self.process_controller.get_status(proc.pid)

        self.assertIsNotNone(status)

    def test_broadcast(self):
        messages = []

        def on_broadcast_receive(**msg):
            messages.append(msg)

        self.communicator.add_broadcast_subscriber(on_broadcast_receive)

        proc = utils.DummyProcess(communicator=self.communicator)
        proc.execute()

        expected_subjects = []
        for i, state in enumerate(utils.DummyProcess.EXPECTED_STATE_SEQUENCE):
            from_state = utils.DummyProcess.EXPECTED_STATE_SEQUENCE[i - 1].value if i != 0 else None
            expected_subjects.append('state_changed.{}.{}'.format(from_state, state.value))

        for i, message in enumerate(messages):
            self.assertEqual(message['subject'], expected_subjects[i])


@unittest.skipIf(not aio_pika, 'Requires pika library and RabbitMQ')
class TestRemoteProcessThreadController(unittest.TestCase):

    def setUp(self):
        super().setUp()

        message_exchange = '{}.{}'.format(self.__class__.__name__, shortuuid.uuid())
        task_exchange = '{}.{}'.format(self.__class__.__name__, shortuuid.uuid())
        task_queue = '{}.{}'.format(self.__class__.__name__, shortuuid.uuid())

        self.communicator = kiwipy.rmq.connect(
            connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
            message_exchange=message_exchange,
            task_exchange=task_exchange,
            task_queue=task_queue,
            testing_mode=True
        )

        self.process_controller = process_comms.RemoteProcessThreadController(self.communicator)

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.communicator.stop()
        super().tearDown()

    @pytest.mark.asyncio
    async def test_pause(self):
        proc = utils.WaitForSignalProcess(communicator=self.communicator)

        # Send a pause message
        pause_future = self.process_controller.pause_process(proc.pid)
        self.assertIsInstance(pause_future, kiwipy.Future)
        future = await asyncio.wrap_future(pause_future)
        result = future.result()
        self.assertIsInstance(result, bool)

        # Check that it all went well
        self.assertTrue(result)
        self.assertTrue(proc.paused)

    @pytest.mark.asyncio
    async def test_pause_all(self):
        """Test pausing all processes on a communicator"""
        procs = []
        for _ in range(10):
            procs.append(utils.WaitForSignalProcess(communicator=self.communicator))

        self.process_controller.pause_all("Slow yo' roll")
        # Wait until they are all paused
        await utils.wait_util(lambda: all([proc.paused for proc in procs]))

    @pytest.mark.asyncio
    async def test_play_all(self):
        """Test pausing all processes on a communicator"""
        procs = []
        for _ in range(10):
            proc = utils.WaitForSignalProcess(communicator=self.communicator)
            procs.append(proc)
            proc.pause('hold tight')

        self.assertTrue(all([proc.paused for proc in procs]))
        self.process_controller.play_all()
        # Wait until they are all paused
        await utils.wait_util(lambda: all([not proc.paused for proc in procs]))

    @pytest.mark.asyncio
    async def test_play(self):
        proc = utils.WaitForSignalProcess(communicator=self.communicator)
        self.assertTrue(proc.pause())

        # Send a play message
        play_future = self.process_controller.play_process(proc.pid)
        # Allow the process to respond to the request
        result = await asyncio.wrap_future(play_future)

        # Check that all is as we expect
        self.assertTrue(result)
        self.assertEqual(proc.state, plumpy.ProcessState.CREATED)

    @pytest.mark.asyncio
    async def test_kill(self):
        proc = utils.WaitForSignalProcess(communicator=self.communicator)

        # Send a kill message
        kill_future = self.process_controller.kill_process(proc.pid)
        # Allow the process to respond to the request
        result = await asyncio.wrap_future(kill_future)

        # Check the outcome
        self.assertTrue(result)
        # Occasionally fail
        self.assertEqual(proc.state, plumpy.ProcessState.KILLED)

    @pytest.mark.asyncio
    async def test_kill_all(self):
        """Test pausing all processes on a communicator"""
        procs = []
        for _ in range(10):
            procs.append(utils.WaitForSignalProcess(communicator=self.communicator))

        self.process_controller.kill_all('bang bang, I shot you down')
        await utils.wait_util(lambda: all([proc.killed() for proc in procs]))
        self.assertTrue(all([proc.state == plumpy.ProcessState.KILLED for proc in procs]))

    @pytest.mark.asyncio
    async def test_status(self):
        proc = utils.WaitForSignalProcess(communicator=self.communicator)
        # Run the process in the background
        asyncio.ensure_future(proc.step_until_terminated())

        # Send a status message
        status_future = self.process_controller.get_status(proc.pid)
        # Let the process respond
        status = await asyncio.wrap_future(status_future)

        self.assertIsNotNone(status)
