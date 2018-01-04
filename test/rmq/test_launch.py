import unittest
import uuid
import shutil
import tempfile

import plum.rmq
import plum.rmq.launch
import plum.test_utils
from plum import test_utils
from test.test_rmq import _HAS_PIKA
from test.util import TestCaseWithLoop

if _HAS_PIKA:
    import pika.exceptions
    from plum.rmq import ProcessLaunchPublisher, ProcessLaunchSubscriber


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestTaskControllerAndRunner(TestCaseWithLoop):
    def setUp(self):
        super(TestTaskControllerAndRunner, self).setUp()

        self.connector = plum.rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        self.queue_name = "{}.{}.tasks".format(self.__class__.__name__, uuid.uuid4())

        self.subscriber = ProcessLaunchSubscriber(self.connector, self.queue_name, testing_mode=True)
        self.publisher = ProcessLaunchPublisher(self.connector, self.queue_name, testing_mode=True)

        self.connector.connect()

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.connector.close()
        super(TestTaskControllerAndRunner, self).tearDown()

    def test_simple_launch(self):
        """Test simply launching a valid process"""
        launch_future = self.publisher.launch(test_utils.DummyProcessWithOutput)
        result = plum.run_until_complete(launch_future)
        self.assertIsNotNone(result)

    def test_simple_continue(self):
        tmppath = tempfile.mkdtemp()
        try:
            persister = plum.PicklePersister(tmppath)

            process = test_utils.WaitForSignalProcess()
            persister.save_checkpoint(process)
            pid = process.pid
            del process

            subscriber = ProcessLaunchSubscriber(
                self.connector,
                self.queue_name,
                testing_mode=True,
                persister=persister)
            future = self.publisher.continue_process(pid)
            self.assertTrue(plum.run_until_complete(future))
        finally:
            shutil.rmtree(tmppath)