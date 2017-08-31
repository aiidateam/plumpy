import unittest
import uuid

from plum import loop_factory
from plum.process import ProcessState
from plum.test_utils import WaitForSignalProcess
from plum.wait_ons import run_until
from test.util import TestCase
from . import utils

if utils._HAS_PIKA:
    import pika
    from plum.rmq.control import ProcessControlPublisher, ProcessControlSubscriber


@unittest.skipIf(not utils._HAS_PIKA, "Requires pika library and RabbitMQ")
class TestControl(TestCase):
    def setUp(self):
        super(TestControl, self).setUp()

        self.loop = loop_factory()

        self._connection = self._create_connection()
        self.exchange = "{}.{}.control".format(self.__class__, uuid.uuid4())

        self.publisher = self.loop.run_until_complete(
            self.loop.create_inserted(ProcessControlPublisher, self._connection, exchange=self.exchange)
        )
        self.subscriber = self.loop.run_until_complete(
            self.loop.create_inserted(ProcessControlSubscriber, self._connection, exchange=self.exchange)
        )

    def tearDown(self):
        super(TestControl, self).tearDown()
        self._connection.close()

    def test_pause(self):
        # Create the process and wait until it is waiting
        p = self.loop.create(WaitForSignalProcess)

        run_until(p, ProcessState.WAITING, self.loop)

        # Send a message asking the process to pause
        self.loop.run_until_complete(self.publisher.pause_process(p.pid))
        self.assertFalse(p.is_playing())

    def test_pause_play(self):
        # Create the process and wait until it is waiting
        p = ~self.loop.create_inserted(WaitForSignalProcess)

        # Playing
        self.assertTrue(p.is_playing())

        # Pause
        # Send a message asking the process to pause
        self.loop.run_until_complete(self.publisher.pause_process(p.pid))
        self.assertFalse(p.is_playing())

        # Now ask it to continue
        self.loop.run_until_complete(self.publisher.play_process(p.pid))
        self.assertTrue(p.is_playing())

    def test_abort(self):
        # Create the process and wait until it is waiting
        proc = self.loop.create(WaitForSignalProcess)
        run_until(proc, ProcessState.WAITING, self.loop)

        # Send a message asking the process to abort, this gives back a future
        # representing the actual abort itself
        fut = ~self.publisher.abort_process(proc.pid, msg='Farewell')
        # Now run until actually aborted
        self.assertTrue(~fut)

        # Check the resulting state
        self.assertTrue(proc.has_aborted())
        self.assertEqual(proc.get_abort_msg(), 'Farewell')

    def test_abort_already_aborted(self):
        # Create the process and wait until it is waiting
        proc = ~self.loop.create_inserted(WaitForSignalProcess)

        # Now abort the process
        proc.abort()

        # Send a message asking the process to abort, this gives back a future
        # representing the actual abort itself
        # Now run until actually aborted
        fut = ~self.publisher.abort_process(proc.pid, msg='Farewell')
        self.assertTrue(~fut)

    def _create_connection(self):
        return pika.BlockingConnection()
