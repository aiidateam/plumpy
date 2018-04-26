import unittest
import uuid

from kiwipy import rmq
import plumpy.rmq
import plumpy.test_utils
from test.utils import TestCaseWithLoop
from plumpy import test_utils

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

        self.communicator.connect()

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.communicator.disconnect()
        self.connector.disconnect()
        super(TestProcessReceiver, self).tearDown()

    def test_pause(self):
        proc = plumpy.test_utils.WaitForSignalProcess(communicator=self.communicator)
        proc.loop().add_callback(proc.step_until_terminated)
        # Send a pause message
        result = self.communicator.rpc_send_and_wait(proc.pid, plumpy.PAUSE_MSG)

        self.assertTrue(proc.paused)

    def test_play(self):
        proc = plumpy.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a play message
        result = self.communicator.rpc_send_and_wait(proc.pid, plumpy.PLAY_MSG)
        self.assertTrue(result)

        self.assertEqual(proc.state, plumpy.ProcessState.CREATED)

    def test_kill(self):
        proc = plumpy.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a kill message
        result = self.communicator.rpc_send_and_wait(proc.pid, plumpy.KILL_MSG)

        self.assertEqual(proc.state, plumpy.ProcessState.KILLED)

    def test_status(self):
        proc = plumpy.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a status message
        status = self.communicator.rpc_send_and_wait(proc.pid, plumpy.STATUS_MSG)

        self.assertIsNotNone(status)

    def test_pause_action(self):
        proc = plumpy.test_utils.WaitForSignalProcess(communicator=self.communicator)
        proc.loop().add_callback(proc.step_until_terminated)
        # Send a pause message
        action = plumpy.PauseAction(proc.pid)
        action.execute(self.communicator)
        self.communicator.await(action, timeout=AWAIT_TIMEOUT)

        self.assertTrue(proc.paused)

    def test_play_action(self):
        proc = plumpy.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a play message
        action = plumpy.PlayAction(proc.pid)
        action.execute(self.communicator)
        self.communicator.await(action, timeout=AWAIT_TIMEOUT)

        self.assertEqual(proc.state, plumpy.ProcessState.CREATED)

    def test_kill_action(self):
        proc = plumpy.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a kill message
        action = plumpy.KillAction(proc.pid)
        action.execute(self.communicator)
        self.communicator.await(action, timeout=AWAIT_TIMEOUT)

        self.assertEqual(proc.state, plumpy.ProcessState.KILLED)

    def test_status(self):
        proc = plumpy.test_utils.WaitForSignalProcess(communicator=self.communicator)
        # Send a status message
        action = plumpy.StatusAction(proc.pid)
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
