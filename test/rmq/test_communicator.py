import shutil
import tempfile
import unittest
import uuid

from plum import rmq
import plum.rmq
import plum.rmq.launch
import plum.test_utils
from test.test_rmq import _HAS_PIKA
from test.utils import TestCaseWithLoop

from plum import test_utils

if _HAS_PIKA:
    import pika.exceptions


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestCommunicator(TestCaseWithLoop):
    def setUp(self):
        super(TestCommunicator, self).setUp()

        self.connector = rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        self.exchange_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())
        self.task_queue_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())

        self.communicator = rmq.RmqCommunicator(
            self.connector,
            exchange_name=self.exchange_name,
            task_queue=self.task_queue_name,
            testing_mode=True
        )

        self.connector.connect()
        # Run the loop until until both are ready
        plum.run_until_complete(self.communicator.initialised_future())

    def tearDown(self):
        self.communicator.close()
        # Close the connector before calling super because it will
        # close the loop
        self.connector.close()
        super(TestCommunicator, self).tearDown()

    def test_rpc_send(self):
        """ Testing making an RPC message and receiving a response """
        MSG = {'test': 5}
        RESPONSE = 'response'
        messages_received = plum.Future()

        class Receiver(plum.Receiver):
            def on_rpc_receive(self, msg):
                messages_received.set_result(msg)
                return RESPONSE

            def on_broadcast_receive(self, msg):
                pass

        receiver = Receiver()
        self.communicator.register_receiver(receiver, 'receiver')

        # Send and make sure we get the message
        future = self.communicator.rpc_send('receiver', MSG)
        result = plum.run_until_complete(messages_received, self.loop)
        self.assertEqual(result, MSG)

        # Now make sure we get the response
        response = plum.run_until_complete(future)
        self.assertEqual(response, RESPONSE)


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestTaskActions(TestCaseWithLoop):
    def setUp(self):
        super(TestTaskActions, self).setUp()

        self.connector = plum.rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        exchange_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())
        queue_name = "{}.{}.tasks".format(self.__class__.__name__, uuid.uuid4())

        self.communicator = rmq.RmqCommunicator(
            self.connector,
            exchange_name=exchange_name,
            task_queue=queue_name,
            testing_mode=True
        )

        self._tmppath = tempfile.mkdtemp()
        self.persister = plum.PicklePersister(self._tmppath)
        # Add a launch task receiver
        self.communicator.add_task_receiver(plum.ProcessLauncher(self.loop, persister=self.persister))

        self.connector.connect()
        # Run the loop until until ready
        plum.run_until_complete(self.communicator.initialised_future())

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.connector.close()
        super(TestTaskActions, self).tearDown()
        shutil.rmtree(self._tmppath)

    def test_launch(self):
        # Add a launch task receiver
        action = plum.LaunchProcessAction(plum.test_utils.DummyProcess)
        action.execute(self.communicator)
        result = plum.run_until_complete(action, self.loop)
        self.assertIsNotNone(result)

    def test_launch_nowait(self):
        # Launch, and don't wait, just get the pid
        action = plum.LaunchProcessAction(plum.test_utils.DummyProcess, nowait=True)
        action.execute(self.communicator)
        result = plum.run_until_complete(action, self.loop)
        self.assertIsInstance(result, uuid.UUID)

    def test_execute_action(self):
        """ Test the process execute action """
        action = plum.ExecuteProcessAction(test_utils.DummyProcessWithOutput)
        action.execute(self.communicator)
        result = plum.run_until_complete(action)
        self.assertEqual(result, test_utils.DummyProcessWithOutput.EXPECTED_OUTPUTS)

    def test_launch_many(self):
        """Test launching multiple processes"""
        num_to_launch = 10

        launch_futures = []
        for _ in range(num_to_launch):
            action = plum.LaunchProcessAction(test_utils.DummyProcessWithOutput)
            action.execute(self.communicator)
            launch_futures.append(action)

        results = plum.run_until_complete(plum.gather(*launch_futures))
        for result in results:
            self.assertIsInstance(result, uuid.UUID)

    def test_continue(self):
        process = test_utils.DummyProcessWithOutput()
        self.persister.save_checkpoint(process)
        pid = process.pid
        del process

        action = plum.ContinueProcessAction(pid)
        action.execute(self.communicator)
        result = plum.run_until_complete(action)
        self.assertEqual(result, test_utils.DummyProcessWithOutput.EXPECTED_OUTPUTS)
