import unittest
import uuid
import shutil
import tempfile

from plum import rmq
import plum.rmq
import plum.rmq.launch
from plum.rmq import launch
import plum.test_utils
from plum import test_utils
from test.test_rmq import _HAS_PIKA
from test.utils import TestCaseWithLoop

if _HAS_PIKA:
    import pika.exceptions
    from plum.rmq import RmqTaskPublisher, RmqTaskSubscriber


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestTaskControllerAndRunner(TestCaseWithLoop):
    def setUp(self):
        super(TestTaskControllerAndRunner, self).setUp()

        self.connector = plum.rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        self.exchange_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())
        self.queue_name = "{}.{}.tasks".format(self.__class__.__name__, uuid.uuid4())

        self.subscriber = RmqTaskSubscriber(
            self.connector,
            exchange_name=self.exchange_name,
            task_queue_name=self.queue_name,
            testing_mode=True)
        self.publisher = RmqTaskPublisher(
            self.connector,
            exchange_name=self.exchange_name,
            task_queue_name=self.queue_name,
            testing_mode=True)

        self.connector.connect()
        # Run the loop until until both are ready
        plum.run_until_complete(
            plum.gather(self.subscriber.initialised_future(),
                        self.publisher.initialised_future()))

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.connector.close()
        super(TestTaskControllerAndRunner, self).tearDown()

    # TODO: Test publisher/subscriber

