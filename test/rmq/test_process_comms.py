import unittest
import uuid

from kiwipy import rmq
import plum.rmq
import plum.test_utils
from test.utils import TestCaseWithLoop
from plum import test_utils

try:
    import pika
except ImportError:
    pika = None

AWAIT_TIMEOUT = 1.


@unittest.skipIf(not pika, "Requires pika library and RabbitMQ")
class TestProcessReceiver(TestCaseWithLoop):
    def setUp(self):
        super(TestProcessReceiver, self).setUp()

        self.connector = rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        exchange_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())

        self.communicator = rmq.RmqCommunicator(
            self.connector,
            exchange_name=exchange_name,
            testing_mode=True,
        )

        self.communicator.init()

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.communicator.disconnect()
        self.connector.disconnect()
        super(TestProcessReceiver, self).tearDown()

    def test_pause(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        proc.play()
        # Send a pause message
        result = self.communicator.rpc_send_and_wait(proc.pid, plum.PAUSE_MSG)

        self.assertEqual(proc.state, plum.ProcessState.PAUSED)

    def test_play(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a play message
        result = self.communicator.rpc_send_and_wait(proc.pid, plum.PLAY_MSG)

        self.assertEqual(proc.state, plum.ProcessState.WAITING)

    def test_cancel(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a cancel message
        result = self.communicator.rpc_send_and_wait(proc.pid, plum.CANCEL_MSG)

        self.assertEqual(proc.state, plum.ProcessState.CANCELLED)

    def test_status(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a status message
        status = self.communicator.rpc_send_and_wait(proc.pid, plum.STATUS_MSG)

        self.assertIsNotNone(status)

    def test_pause_action(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        proc.play()
        # Send a pause message
        action = plum.PauseAction(proc.pid)
        action.execute(self.communicator)
        self.communicator.await(action, timeout=AWAIT_TIMEOUT)

        self.assertEqual(proc.state, plum.ProcessState.PAUSED)

    def test_play_action(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a play message
        action = plum.PlayAction(proc.pid)
        action.execute(self.communicator)
        self.communicator.await(action, timeout=AWAIT_TIMEOUT)

        self.assertEqual(proc.state, plum.ProcessState.WAITING)

    def test_cancel_action(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a cancel message
        action = plum.CancelAction(proc.pid)
        action.execute(self.communicator)
        self.communicator.await(action, timeout=AWAIT_TIMEOUT)

        self.assertEqual(proc.state, plum.ProcessState.CANCELLED)

    def test_status(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a status message
        action = plum.StatusAction(proc.pid)
        action.execute(self.communicator)
        status = self.communicator.await(action, timeout=AWAIT_TIMEOUT)
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
            expected_subjects.append(
                "state_changed.{}.{}".format(from_state, state.value))

        for i, message in enumerate(messages):
            self.assertEqual(message['subject'], expected_subjects[i])
