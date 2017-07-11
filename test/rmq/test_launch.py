import unittest
import uuid

import pika

from plum import loop_factory
from plum.rmq import ProcessLaunchPublisher, ProcessLaunchSubscriber
from plum.test_utils import TEST_PROCESSES
from test.test_rmq import _HAS_PIKA
from test.util import TestCase


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestTaskControllerAndRunner(TestCase):
    def setUp(self):
        super(TestTaskControllerAndRunner, self).setUp()

        try:
            self._connection = pika.BlockingConnection()
        except pika.exceptions.ConnectionClosed:
            self.fail("Couldn't open connection.  Make sure rmq server is running")

        queue = "{}.{}.tasks".format(self.__class__.__name__, uuid.uuid4())
        self.publisher = ProcessLaunchPublisher(self._connection, queue=queue)
        self.subscriber = ProcessLaunchSubscriber(self._connection, queue=queue)

        self.loop = loop_factory()
        self.loop.insert(self.publisher)
        self.loop.insert(self.subscriber)

    def tearDown(self):
        self._connection.close()

    def test_launch(self):
        # Try launching some processes
        launch_requests = []
        for proc_class in TEST_PROCESSES:
            launch_requests.append(self.publisher.launch(proc_class))

        # Make sure they have all launched
        launched = []
        for future in launch_requests:
            self.loop.run_until_complete(future)
            launched.append(self.loop.get_object(future.result()['pid']).__class__)

        self.assertEqual(len(launched), len(TEST_PROCESSES))
        self.assertListEqual(TEST_PROCESSES, launched)
