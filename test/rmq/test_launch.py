import apricotpy
import unittest
import uuid

from plum import loop_factory
import plum.test_utils
from plum.test_utils import TEST_PROCESSES
from test.test_rmq import _HAS_PIKA
from test.util import TestCase

if _HAS_PIKA:
    import pika.exceptions
    from plum.rmq import ProcessLaunchPublisher, ProcessLaunchSubscriber


def _create_temporary_queue(connection):
    channel = connection.channel()
    result = channel.queue_declare(exclusive=True)
    return result.method.queue


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestTaskControllerAndRunner(TestCase):
    def setUp(self):
        super(TestTaskControllerAndRunner, self).setUp()

        try:
            self._connection = pika.BlockingConnection()
        except pika.exceptions.ConnectionClosed:
            self.fail("Couldn't open connection.  Make sure rmq server is running")

        self.loop = loop_factory()

        queue = "{}.{}.tasks".format(self.__class__.__name__, uuid.uuid4())

        loop = self.loop
        insert = (
            loop.create_inserted(ProcessLaunchPublisher, self._connection, queue=queue),
            loop.create_inserted(ProcessLaunchSubscriber, self._connection, queue=queue)
        )

        self.publisher, self.subscriber = ~apricotpy.gather(insert, loop)

    def tearDown(self):
        self._connection.close()
        self.loop.close()
        self.loop = None

    def test_launch(self):
        # Try launching a process
        launch = self.publisher.launch(plum.test_utils.WaitForSignalProcess)

        # Make sure it has launched
        response, done = self.loop.run_until_complete(launch)

        proc = self.loop.get_object(response['pid'])
        self.assertEqual(str(proc.pid), proc.pid)
