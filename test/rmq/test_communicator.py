import shutil
import tempfile
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
class TestTaskActions(TestCaseWithLoop):
    def setUp(self):
        super(TestTaskActions, self).setUp()

        self.connector = plumpy.rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        exchange_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())
        queue_name = "{}.{}.tasks".format(self.__class__.__name__, uuid.uuid4())

        self.communicator = rmq.RmqCommunicator(
            self.connector,
            exchange_name=exchange_name,
            task_queue=queue_name,
            testing_mode=True
        )

        self.communicator.init()

        self._tmppath = tempfile.mkdtemp()
        self.persister = plumpy.PicklePersister(self._tmppath)
        # Add a launch task receiver
        self.communicator.add_task_subscriber(plumpy.ProcessLauncher(self.loop, persister=self.persister))

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.connector.disconnect()
        super(TestTaskActions, self).tearDown()
        shutil.rmtree(self._tmppath)

    def test_launch(self):
        # Add a launch task receiver
        action = plumpy.LaunchProcessAction(plumpy.test_utils.DummyProcess)
        action.execute(self.communicator)
        result = self.communicator.await(action, timeout=AWAIT_TIMEOUT)
        self.assertIsNotNone(result)

    def test_launch_nowait(self):
        # Launch, and don't wait, just get the pid
        action = plumpy.LaunchProcessAction(plumpy.test_utils.DummyProcess, nowait=True)
        action.execute(self.communicator)
        result = self.communicator.await(action, timeout=AWAIT_TIMEOUT)
        self.assertIsInstance(result, uuid.UUID)

    def test_execute_action(self):
        """ Test the process execute action """
        action = plumpy.ExecuteProcessAction(test_utils.DummyProcessWithOutput)
        action.execute(self.communicator)
        result = self.communicator.await(action, timeout=AWAIT_TIMEOUT)
        self.assertEqual(result, test_utils.DummyProcessWithOutput.EXPECTED_OUTPUTS)

    def test_execute_action_nowait(self):
        """ Test the process execute action """
        action = plumpy.ExecuteProcessAction(test_utils.DummyProcessWithOutput, nowait=True)
        action.execute(self.communicator)
        result = self.communicator.await(action, timeout=AWAIT_TIMEOUT)
        self.assertIsInstance(result, uuid.UUID)

    def test_launch_many(self):
        """Test launching multiple processes"""
        num_to_launch = 10

        launch_futures = []
        for _ in range(num_to_launch):
            action = plumpy.LaunchProcessAction(test_utils.DummyProcessWithOutput)
            action.execute(self.communicator)
            launch_futures.append(action)

        results = self.communicator.await(plumpy.gather(*launch_futures), timeout=AWAIT_TIMEOUT)
        for result in results:
            self.assertIsInstance(result, uuid.UUID)

    def test_continue(self):
        process = test_utils.DummyProcessWithOutput()
        self.persister.save_checkpoint(process)
        pid = process.pid
        del process

        action = plumpy.ContinueProcessAction(pid)
        action.execute(self.communicator)
        result = self.communicator.await(action, timeout=AWAIT_TIMEOUT)
        self.assertEqual(result, test_utils.DummyProcessWithOutput.EXPECTED_OUTPUTS)
