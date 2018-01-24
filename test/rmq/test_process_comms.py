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
            blocking_mode=False,
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
        self.communicator.await_response(future)

        self.assertEqual(proc.state, plum.ProcessState.PAUSED)

    def test_play(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a play message
        future = self.communicator.rpc_send(proc.pid, plum.PLAY_MSG)
        self.communicator.await_response(future)

        self.assertEqual(proc.state, plum.ProcessState.WAITING)

    def test_cancel(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a cancel message
        future = self.communicator.rpc_send(proc.pid, plum.CANCEL_MSG)
        self.communicator.await_response(future)

        self.assertEqual(proc.state, plum.ProcessState.CANCELLED)

    def test_status(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a status message
        future = self.communicator.rpc_send(proc.pid, plum.STATUS_MSG)
        status = self.communicator.await_response(future)

        self.assertIsNotNone(status)

    def test_pause_action(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        proc.play()
        # Send a pause message
        action = plum.PauseAction(proc.pid)
        action.execute(self.communicator)
        self.communicator.await_response(action)

        self.assertEqual(proc.state, plum.ProcessState.PAUSED)

    def test_play_action(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a play message
        action = plum.PlayAction(proc.pid)
        action.execute(self.communicator)
        self.communicator.await_response(action)

        self.assertEqual(proc.state, plum.ProcessState.WAITING)

    def test_cancel_action(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a cancel message
        action = plum.CancelAction(proc.pid)
        action.execute(self.communicator)
        self.communicator.await_response(action)

        self.assertEqual(proc.state, plum.ProcessState.CANCELLED)

    def test_status(self):
        proc = plum.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a status message
        action = plum.StatusAction(proc.pid)
        action.execute(self.communicator)
        status = self.communicator.await_response(action)
        self.assertIsNotNone(status)

    def test_broadcast(self):
        messages = []

        def on_broadcast_receive(**msg):
            messages.append(msg)

        self.communicator.add_broadcast_subscriber(on_broadcast_receive)
        proc = test_utils.DummyProcess(communicator=self.communicator)
        proc.execute()

        expected_subjects = []
        for i, state in enumerate(test_utils.DummyProcess.EXPECTED_STATE_SEQUENCE):
            from_state = test_utils.DummyProcess.EXPECTED_STATE_SEQUENCE[i - 1].value if i != 0 else None
            expected_subjects.append(
                "state_changed.{}.{}".format(from_state, state.value))

        for i, message in enumerate(messages):
            self.assertEqual(message['subject'], expected_subjects[i])
