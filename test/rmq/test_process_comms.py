# -*- coding: utf-8 -*-
import unittest

import shortuuid
from tornado import testing
import kiwipy.rmq

import plumpy
import plumpy.communications
from plumpy import process_comms
from test import test_utils
from .. import utils

try:
    import pika
except ImportError:
    pika = None

AWAIT_TIMEOUT = testing.get_async_test_timeout()


@unittest.skipIf(not pika, 'Requires pika library and RabbitMQ')
class TestRemoteProcessController(utils.AsyncTestCase):

    def setUp(self):
        super(TestRemoteProcessController, self).setUp()

        self.init_communicator()
        self.process_controller = process_comms.RemoteProcessController(self.communicator)

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.communicator.stop()
        super(TestRemoteProcessController, self).tearDown()

    @testing.gen_test
    def test_pause(self):
        proc = test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Run the process in the background
        proc.loop().add_callback(proc.step_until_terminated)
        # Send a pause message
        result = yield self.process_controller.pause_process(proc.pid)

        # Check that it all went well
        self.assertTrue(result)
        self.assertTrue(proc.paused)

    @testing.gen_test
    def test_play(self):
        proc = test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Run the process in the background
        proc.loop().add_callback(proc.step_until_terminated)
        self.assertTrue(proc.pause())

        # Send a play message
        result = yield self.process_controller.play_process(proc.pid)

        # Check that all is as we expect
        self.assertTrue(result)
        self.assertEqual(proc.state, plumpy.ProcessState.WAITING)

    @testing.gen_test
    def test_kill(self):
        proc = test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Run the process in the event loop
        self.loop.add_callback(proc.step_until_terminated)

        # Send a kill message and wait for it to be done
        result = yield self.process_controller.kill_process(proc.pid)

        # Check the outcome
        self.assertTrue(result)
        self.assertEqual(proc.state, plumpy.ProcessState.KILLED)

    @testing.gen_test
    def test_status(self):
        proc = test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Run the process in the background
        proc.loop().add_callback(proc.step_until_terminated)

        # Send a status message
        status = yield self.process_controller.get_status(proc.pid)

        self.assertIsNotNone(status)

    def test_broadcast(self):
        messages = []

        def on_broadcast_receive(**msg):
            messages.append(msg)

        self.communicator.add_broadcast_subscriber(on_broadcast_receive)
        proc = test_utils.DummyProcess(loop=self.loop, communicator=self.communicator)
        proc.execute()

        expected_subjects = []
        for i, state in enumerate(test_utils.DummyProcess.EXPECTED_STATE_SEQUENCE):
            from_state = test_utils.DummyProcess.EXPECTED_STATE_SEQUENCE[i - 1].value if i != 0 else None
            expected_subjects.append('state_changed.{}.{}'.format(from_state, state.value))

        for i, message in enumerate(messages):
            self.assertEqual(message['subject'], expected_subjects[i])


@unittest.skipIf(not pika, 'Requires pika library and RabbitMQ')
class TestRemoteProcessThreadController(testing.AsyncTestCase):

    def setUp(self):
        super(TestRemoteProcessThreadController, self).setUp()

        self.loop = self.io_loop

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
        super(TestRemoteProcessThreadController, self).tearDown()

    @testing.gen_test
    def test_pause(self):
        proc = test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a pause message
        pause_future = yield self.process_controller.pause_process(proc.pid)
        self.assertIsInstance(pause_future, kiwipy.Future)
        result = yield pause_future
        self.assertIsInstance(result, bool)

        # Check that it all went well
        self.assertTrue(result)
        self.assertTrue(proc.paused)

    @testing.gen_test
    def test_pause_all(self):
        """Test pausing all processes on a communicator"""
        procs = []
        for _ in range(10):
            procs.append(test_utils.WaitForSignalProcess(communicator=self.communicator))

        self.process_controller.pause_all("Slow yo' roll")
        # Wait until they are all paused
        yield utils.wait_util(lambda: all([proc.paused for proc in procs]))

    @testing.gen_test
    def test_play_all(self):
        """Test pausing all processes on a communicator"""
        procs = []
        for _ in range(10):
            proc = test_utils.WaitForSignalProcess(communicator=self.communicator)
            procs.append(proc)
            proc.pause('hold tight')

        self.assertTrue(all([proc.paused for proc in procs]))
        self.process_controller.play_all()
        # Wait until they are all paused
        yield utils.wait_util(lambda: all([not proc.paused for proc in procs]))

    @testing.gen_test
    def test_play(self):
        proc = test_utils.WaitForSignalProcess(communicator=self.communicator)
        self.assertTrue(proc.pause())

        # Send a play message
        play_future = self.process_controller.play_process(proc.pid)
        # Allow the process to respond to the request
        result = yield play_future

        # Check that all is as we expect
        self.assertTrue(result)
        self.assertEqual(proc.state, plumpy.ProcessState.CREATED)

    @testing.gen_test
    def test_kill(self):
        proc = test_utils.WaitForSignalProcess(communicator=self.communicator)

        # Send a kill message
        kill_future = yield self.process_controller.kill_process(proc.pid)
        # Allow the process to respond to the request
        result = yield kill_future

        # Check the outcome
        self.assertTrue(result)
        # Occasionally fail
        self.assertEqual(proc.state, plumpy.ProcessState.KILLED)

    @testing.gen_test
    def test_kill_all(self):
        """Test pausing all processes on a communicator"""
        procs = []
        for _ in range(10):
            procs.append(test_utils.WaitForSignalProcess(communicator=self.communicator))

        self.process_controller.kill_all('bang bang, I shot you down')
        yield utils.wait_util(lambda: all([proc.killed() for proc in procs]))
        self.assertTrue(all([proc.state == plumpy.ProcessState.KILLED for proc in procs]))

    @testing.gen_test
    def test_status(self):
        proc = test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Run the process in the background
        proc.loop().add_callback(proc.step_until_terminated)

        # Send a status message
        status_future = self.process_controller.get_status(proc.pid)
        # Let the process respond
        status = yield status_future

        self.assertIsNotNone(status)
