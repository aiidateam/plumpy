import unittest
import uuid

import plum
from plum import rmq
from plum.test_utils import WaitForSignalProcess
from test.util import TestCaseWithLoop
from . import utils

if utils._HAS_PIKA:
    import pika
    from plum.rmq.control import ProcessControlPublisher


@unittest.skipIf(not utils._HAS_PIKA, "Requires pika library and RabbitMQ")
class TestControl(TestCaseWithLoop):
    def setUp(self):
        super(TestControl, self).setUp()

        self.connector = rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        self.exchange_name = "{}.{}.control".format(self.__class__.__name__, uuid.uuid4())

        self.publisher = ProcessControlPublisher(
            self.connector, exchange_name=self.exchange_name)

        self.connector.connect()

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.connector.close()
        super(TestControl, self).tearDown()

    def test_no_controller(self):
        """ Test sending a message when there is no controller listening """
        process = WaitForSignalProcess()
        with self.assertRaises(RuntimeError):
            plum.run_until_complete(self.publisher.pause_process(process.pid))

    def test_missing_controller(self):
        """
        Test sending a message when there is a controller but for a
        different process
        """
        proc1 = WaitForSignalProcess()
        proc2 = WaitForSignalProcess()
        controller = self._create_rmq_controller(proc1)
        with self.assertRaises(RuntimeError):
            plum.run_until_complete(self.publisher.pause_process(proc2.pid))

    def test_pause(self):
        # Create the process and wait until it is waiting
        process = WaitForSignalProcess()
        controller = self._create_rmq_controller(process)

        process.execute(True)

        # Send a message asking the process to pause
        pause = self.publisher.pause_process(process.pid)
        plum.run_until_complete(pause)
        self.assertEqual(process.state, plum.ProcessState.PAUSED)

    def test_pause_play(self):
        # Create the process and wait until it is waiting
        process = WaitForSignalProcess()
        controller = self._create_rmq_controller(process)

        process.execute(True)

        # Send a message asking the process to pause
        pause = self.publisher.pause_process(process.pid)
        plum.run_until_complete(pause)
        self.assertEqual(process.state, plum.ProcessState.PAUSED)

        # Now ask it to play
        play = self.publisher.play_process(process.pid)
        plum.run_until_complete(play)
        self.assertEqual(process.state, plum.ProcessState.WAITING)

    def test_cancel(self):
        # Create the process and wait until it is waiting
        proc = WaitForSignalProcess()
        controller = self._create_rmq_controller(proc)
        proc.execute(True)

        # Send a message asking the process to cancel, this gives back a future
        # representing the actual cancel operation itself

        cancel = self.publisher.cancel_process(proc.pid, msg='Farewell')
        plum.run_until_complete(cancel)

        # Check the resulting state
        self.assertTrue(proc.cancelled())
        self.assertEqual(proc.cancelled_msg(), 'Farewell')

    def test_abort_already_aborted(self):
        # Create the process and wait until it is waiting
        proc = WaitForSignalProcess()
        controller = self._create_rmq_controller(proc)

        # Now cancel the process
        proc.cancel()

        # Send a message asking the process to abort, this gives back a future
        # representing the actual abort itself
        # Now run until actually aborted
        abort = self.publisher.cancel_process(proc.pid, msg='Farewell')
        plum.run_until_complete(abort)

    def _create_rmq_controller(self, process):
        return rmq.RmqProcessController(process, self.connector, exchange_name=self.exchange_name)
