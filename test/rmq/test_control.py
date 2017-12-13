import unittest
import uuid

import plum
from plum.test_utils import WaitForSignalProcess
from plum.wait_ons import run_until
from test.util import TestCaseWithLoop
from . import utils
from .. import util as test_utils

if utils._HAS_PIKA:
    import pika
    from plum.rmq.control import ProcessControlPublisher, ProcessControlSubscriber


#@unittest.skipIf(not utils._HAS_PIKA, "Requires pika library and RabbitMQ")
@unittest.skip("Refactoring RMQ support")
class TestControl(TestCaseWithLoop):
    def setUp(self):
        super(TestControl, self).setUp()

        self._connection = self._create_connection()
        self.exchange = "{}.{}.control".format(self.__class__, uuid.uuid4())

        self.publisher = self.loop.create(
            ProcessControlPublisher, self._connection, exchange=self.exchange)
        self.subscriber = self.loop.create(
            ProcessControlSubscriber, self._connection, exchange=self.exchange)

    def tearDown(self):
        super(TestControl, self).tearDown()
        self._connection.close()

    def test_pause(self):
        # Create the process and wait until it is waiting
        p = WaitForSignalProcess().play()

        run_until(p, plum.ProcessState.WAITING, self.loop)

        # Send a message asking the process to pause
        pause = self.publisher.pause_process(p.pid)
        self.loop.run_until_complete(test_utils.HansKlok(pause))
        self.assertFalse(p.is_playing())

    def test_pause_play(self):
        # Create the process and wait until it is waiting
        p = WaitForSignalProcess().play()

        # Playing
        self.assertTrue(p.is_playing())

        # Pause
        # Send a message asking the process to pause
        pause = self.publisher.pause_process(p.pid)
        self.loop.run_until_complete(test_utils.HansKlok(pause))
        self.assertFalse(p.is_playing())

        # Now ask it to continue
        play = self.publisher.play_process(p.pid)
        self.loop.run_until_complete(test_utils.HansKlok(play))
        self.assertTrue(p.is_playing())

    def test_abort(self):
        # Create the process and wait until it is waiting
        proc = WaitForSignalProcess().play()
        run_until(proc, plum.ProcessState.WAITING, self.loop)

        # Send a message asking the process to abort, this gives back a future
        # representing the actual abort itself

        abort = self.publisher.abort_process(proc.pid, msg='Farewell')
        fut = self.loop.run_until_complete(test_utils.HansKlok(abort))
        # Now run until actually aborted
        self.assertTrue(~fut)

        # Check the resulting state
        self.assertTrue(proc.has_aborted())
        self.assertEqual(proc.get_abort_msg(), 'Farewell')

    def test_abort_already_aborted(self):
        # Create the process and wait until it is waiting
        proc = self.loop.create(WaitForSignalProcess)

        # Now abort the process
        proc.abort()

        # Send a message asking the process to abort, this gives back a future
        # representing the actual abort itself
        # Now run until actually aborted
        abort = self.publisher.abort_process(proc.pid, msg='Farewell')
        fut = self.loop.run_until_complete(test_utils.HansKlok(abort))
        self.assertTrue(~fut)

    def _create_connection(self):
        return pika.BlockingConnection()
