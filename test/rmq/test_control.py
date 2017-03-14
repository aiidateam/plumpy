import unittest
import uuid

import pika
import pika.exceptions

from plum.process import ProcessState
from plum.process_manager import ProcessManager
from plum.rmq.control import ProcessControlPublisher, ProcessControlSubscriber
from plum.test_utils import WaitForSignalProcess
from plum.rmq.util import SubscriberThread
from plum.wait_ons import wait_until
from test.test_rmq import _HAS_PIKA
from test.util import TestCase


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestProcessControl(TestCase):
    def setUp(self):
        super(TestProcessControl, self).setUp()

        self._connection = self._create_connection()
        self.procman = ProcessManager()
        self.queue = "{}.{}.control".format(self.__class__, uuid.uuid4())
        self.publisher = ProcessControlPublisher(self._connection, queue=self.queue)

        self.subscriber_thread = SubscriberThread(
            self._create_connection, self._create_control_subscriber)
        self.subscriber_thread.set_poll_time(0.1)
        self.subscriber_thread.start()

    def tearDown(self):
        self.procman.shutdown()
        super(TestProcessControl, self).tearDown()
        self.subscriber_thread.stop()
        self.subscriber_thread.join()
        self._connection.close()

    def test_pause(self):
        # Create the process and wait until it is waiting
        p = WaitForSignalProcess.new()
        self.procman.start(p)
        wait_until(p, ProcessState.WAITING)
        self.assertTrue(p.is_playing())

        # Send a message asking the process to pause
        self.assertIsNotNone(self.publisher.pause(p.pid, timeout=2.))
        self.assertFalse(p.is_playing())

    def test_pause_play(self):
        # Create the process and wait until it is waiting
        p = WaitForSignalProcess.new()
        self.procman.start(p)
        self.assertTrue(wait_until(p, ProcessState.WAITING, 1))
        self.assertTrue(p.is_playing())

        # Send a message asking the process to pause
        self.assertIsNotNone(self.publisher.pause(p.pid, timeout=2.))
        self.assertFalse(p.is_playing())

        # Now ask it to continue
        self.assertIsNotNone(self.publisher.play(p.pid, timeout=2.))
        self.assertTrue(p.is_playing())

    def test_abort(self):
        # Create the process and wait until it is waiting
        p = WaitForSignalProcess.new()
        self.procman.start(p)
        wait_until(p, ProcessState.WAITING)
        self.assertTrue(p.is_playing())

        # Send a message asking the process to abort
        self.assertIsNotNone(self.publisher.abort(p.pid, timeout=2.))
        self.assertFalse(p.is_playing())
        self.assertTrue(p.has_aborted())

    def _create_connection(self):
        return pika.BlockingConnection()

    def _create_control_subscriber(self, connection):
        return ProcessControlSubscriber(connection, queue=self.queue, process_manager=self.procman)
