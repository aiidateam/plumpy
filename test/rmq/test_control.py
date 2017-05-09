import unittest
import uuid

import pika
import pika.exceptions

from plum.process import ProcessState
from plum.process_controller import ProcessController
from plum.rmq.control import ProcessControlPublisher, ProcessControlSubscriber
from plum.test_utils import WaitForSignalProcess
from plum.rmq.util import SubscriberThread
from plum.wait_ons import wait_until
from test.test_rmq import _HAS_PIKA
from test.util import TestCase


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestControl(TestCase):
    def setUp(self):
        super(TestControl, self).setUp()

        self._connection = self._create_connection()
        self.controller = ProcessController()
        self.exchange = "{}.{}.control".format(self.__class__, uuid.uuid4())
        self.publisher = ProcessControlPublisher(self._connection, exchange=self.exchange)

        self.subscriber_thread = SubscriberThread(
            self._create_connection, self._create_control_subscriber)
        self.subscriber_thread.set_poll_time(0.1)
        self.subscriber_thread.start()
        self.subscriber_thread.wait_till_started()

    def tearDown(self):
        self.controller.remove_all(timeout=10.)
        self.assertEqual(self.controller.get_num_processes(), 0, "Failed to abort all processes")
        super(TestControl, self).tearDown()
        self.subscriber_thread.stop()
        self.subscriber_thread.join()
        self._connection.close()

    def test_pause(self):
        # Create the process and wait until it is waiting
        p = WaitForSignalProcess.new()
        self.controller.insert_and_play(p)
        wait_until(p, ProcessState.WAITING)
        self.assertTrue(p.is_playing())

        # Send a message asking the process to pause
        self.assertIsNotNone(self.publisher.pause(p.pid, timeout=5.))
        self.assertFalse(p.is_playing())

    def test_pause_play(self):
        # Create the process and wait until it is waiting
        p = WaitForSignalProcess.new()

        # Play
        self.controller.insert_and_play(p)
        self.assertTrue(wait_until(p, ProcessState.WAITING, 1))
        self.assertTrue(p.is_playing())

        # Pause
        # Send a message asking the process to pause
        self.assertIsNotNone(self.publisher.pause(p.pid, timeout=5.))
        self.assertFalse(p.is_playing())

        # Now ask it to continue
        self.assertIsNotNone(self.publisher.play(p.pid, timeout=5.))
        self.assertTrue(p.is_playing())

    def test_abort(self):
        # Create the process and wait until it is waiting
        p = WaitForSignalProcess.new()
        self.controller.insert_and_play(p)
        self.assertTrue(wait_until(p, ProcessState.WAITING, timeout=2.))
        self.assertTrue(p.is_playing())

        # Send a message asking the process to abort
        self.assertIsNotNone(self.publisher.abort(p.pid, msg='Farewell', timeout=5.))
        self.assertTrue(p.wait(timeout=2.), "Process failed to stop running")
        self.assertFalse(p.is_playing())
        self.assertTrue(p.has_aborted())
        self.assertEqual(p.get_abort_msg(), 'Farewell')

    def _create_connection(self):
        return pika.BlockingConnection()

    def _create_control_subscriber(self, connection):
        return ProcessControlSubscriber(connection, exchange=self.exchange, process_controller=self.controller)
