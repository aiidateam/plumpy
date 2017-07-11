import unittest
import uuid

import pika
import pika.exceptions

from plum import loop_factory
from plum.process import ProcessState
from plum.rmq.control import ProcessControlPublisher, ProcessControlSubscriber
from plum.test_utils import WaitForSignalProcess
from plum.wait_ons import run_until
from test.test_rmq import _HAS_PIKA
from test.util import TestCase


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestControl(TestCase):
    def setUp(self):
        super(TestControl, self).setUp()

        self._connection = self._create_connection()
        self.exchange = "{}.{}.control".format(self.__class__, uuid.uuid4())
        self.publisher = ProcessControlPublisher(self._connection, exchange=self.exchange)
        self.subscriber = ProcessControlSubscriber(self._connection, exchange=self.exchange)

        self.loop = loop_factory()
        self.loop.insert(self.publisher)
        self.loop.insert(self.subscriber)

    def tearDown(self):
        super(TestControl, self).tearDown()
        self._connection.close()

    def test_pause(self):
        # Create the process and wait until it is waiting
        p = self.loop.create_task(WaitForSignalProcess)

        run_until(p, ProcessState.WAITING, self.loop)

        # Send a message asking the process to pause
        self.loop.run_until_complete(self.publisher.pause(p.pid))
        self.assertFalse(p.is_playing())

    def test_pause_play(self):
        # Create the process and wait until it is waiting
        p = self.loop.create_task(WaitForSignalProcess)

        # Playing
        self.assertTrue(p.is_playing())

        # Pause
        # Send a message asking the process to pause
        self.loop.run_until_complete(self.publisher.pause(p.pid))
        self.assertFalse(p.is_playing())

        # Now ask it to continue
        self.loop.run_until_complete(self.publisher.play(p.pid))
        self.assertTrue(p.is_playing())

    def test_abort(self):
        # Create the process and wait until it is waiting
        p = self.loop.create_task(WaitForSignalProcess)
        run_until(p, ProcessState.WAITING, self.loop)

        # Send a message asking the process to abort
        self.loop.run_until_complete(self.publisher.abort(p.pid, msg='Farewell'))

        # Now tick the loop to action the abort
        self.loop.tick()

        # Check the resulting state
        self.assertTrue(p.has_aborted())
        self.assertEqual(p.get_abort_msg(), 'Farewell')

    def _create_connection(self):
        return pika.BlockingConnection()
