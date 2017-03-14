try:
    import pika
    import pika.exceptions
    from plum.rmq.launch import ProcessLaunchPublisher, ProcessLaunchSubscriber
    from plum.rmq.control import ProcessControlSubscriber
    from plum.rmq.status import StatusSubscriber
    from plum.rmq.util import SubscriberThread

    _HAS_PIKA = True
except ImportError:
    _HAS_PIKA = False
import threading
import unittest
import uuid

from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.test_utils import TEST_PROCESSES
from util import TestCase


class Out(object):
    def __init__(self):
        self.runner = None
        self.is_set = threading.Event()


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestTaskControllerAndRunner(TestCase):
    def setUp(self):
        super(TestTaskControllerAndRunner, self).setUp()

        try:
            self._connection = pika.BlockingConnection()
        except pika.exceptions.ConnectionClosed:
            self.fail("Couldn't open connection.  Make sure _rmq server is running")

        queue = "{}.{}.tasks".format(self.__class__.__name__, uuid.uuid4())
        self.publisher = ProcessLaunchPublisher(self._connection, queue=queue)
        self.subscriber = ProcessLaunchSubscriber(self._connection, queue=queue)

    def tearDown(self):
        self._connection.close()

    def test_send(self):
        class RanLogger(ProcessMonitorListener):
            def __init__(self):
                self.ran = []

            def on_monitored_process_registered(self, process):
                self.ran.append(process.__class__)

        l = RanLogger()
        with MONITOR.listen(l):
            # Try sending some processes
            for ProcClass in TEST_PROCESSES:
                self.publisher.launch(ProcClass)

            # Now make them run
            num_ran = 0
            i = 0
            while num_ran < len(TEST_PROCESSES):
                num_ran += self.subscriber.poll(0.2)
                i += 1
                if i > 10:
                    break

            self.assertEqual(num_ran, len(TEST_PROCESSES))

        self.assertListEqual(TEST_PROCESSES, l.ran)


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestRmqThread(TestCase):
    def test_start_stop(self):
        t = SubscriberThread(self._create_connection, self._create_subscribers)
        t.set_poll_time(0.0)
        t.start()
        self.assertTrue(t.wait_till_started(1), "Subscriber thread failed to start")
        t.stop()
        t.join(2)
        self.assertFalse(t.is_alive())

    def _create_connection(self):
        try:
            return pika.BlockingConnection()
        except pika.exceptions.ConnectionClosed:
            self.fail("Couldn't open connection.  Make sure _rmq server is running")

    def _create_subscribers(self, connection):
        subscribers = []
        for c in [ProcessControlSubscriber, StatusSubscriber, ProcessLaunchSubscriber]:
            subscribers.append(c(connection))
        return subscribers