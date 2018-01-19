import unittest
import uuid

from plum import rmq
import plum.rmq
import plum.rmq.launch
import plum.test_utils
from test.test_rmq import _HAS_PIKA
from test.utils import TestCaseWithLoop

if _HAS_PIKA:
    import pika.exceptions


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestProcessReceiver(TestCaseWithLoop):
    def setUp(self):
        super(TestProcessReceiver, self).setUp()

        self.connector = rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        exchange_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())

        self.communicator = rmq.RmqCommunicator(
            self.connector,
            exchange_name=exchange_name,
            testing_mode=True
        )

        self.connector.connect()
        # Run the loop until until both are ready

        plum.run_until_complete(self.communicator.initialised_future())

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.connector.close()
        super(TestProcessReceiver, self).tearDown()

    def test_pause(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        proc.play()
        # Send a pause message
        future = self.communicator.rpc_send(proc.pid, plum.PAUSE_MSG)
        plum.run_until_complete(future)

        self.assertEqual(proc.state, plum.ProcessState.PAUSED)

    def test_play(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a play message
        future = self.communicator.rpc_send(proc.pid, plum.PLAY_MSG)
        plum.run_until_complete(future)

        self.assertEqual(proc.state, plum.ProcessState.WAITING)

    def test_cancel(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a cancel message
        future = self.communicator.rpc_send(proc.pid, plum.CANCEL_MSG)
        plum.run_until_complete(future)

        self.assertEqual(proc.state, plum.ProcessState.CANCELLED)

    def test_status(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a status message
        future = self.communicator.rpc_send(proc.pid, plum.STATUS_MSG)
        status = plum.run_until_complete(future)

        self.assertIsNotNone(status)

    def test_pause_action(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        proc.play()
        # Send a pause message
        action = plum.PauseAction(proc.pid)
        action.execute(self.communicator)
        plum.run_until_complete(action)

        self.assertEqual(proc.state, plum.ProcessState.PAUSED)

    def test_play_action(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a play message
        action = plum.PlayAction(proc.pid)
        action.execute(self.communicator)
        plum.run_until_complete(action)

        self.assertEqual(proc.state, plum.ProcessState.WAITING)

    def test_cancel_action(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a cancel message
        action = plum.CancelAction(proc.pid)
        action.execute(self.communicator)
        plum.run_until_complete(action)

        self.assertEqual(proc.state, plum.ProcessState.CANCELLED)

    def test_status(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a status message
        action = plum.StatusAction(proc.pid)
        action.execute(self.communicator)
        status = plum.run_until_complete(action)
        self.assertIsNotNone(status)
