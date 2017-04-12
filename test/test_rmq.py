try:
    import pika
    import pika.exceptions
    from plum.rmq.launch import ProcessLaunchPublisher, ProcessLaunchSubscriber
    from plum.rmq.control import ProcessControlSubscriber
    from plum.rmq.status import ProcessStatusSubscriber
    from plum.rmq.util import SubscriberThread

    _HAS_PIKA = True
except ImportError:
    _HAS_PIKA = False
import threading
import unittest
import uuid
import warnings

from plum.process_manager import ProcessManager
from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.test_utils import TEST_PROCESSES
from test.util import TestCase


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

        self.procman = ProcessManager()
        queue = "{}.{}.tasks".format(self.__class__.__name__, uuid.uuid4())
        self.publisher = ProcessLaunchPublisher(self._connection, queue=queue)
        self.subscriber = ProcessLaunchSubscriber(
            self._connection, queue=queue, process_manager=self.procman)

    def tearDown(self):
        self._connection.close()
        num_procs = self.procman.get_num_processes()
        if num_procs != 0:
            warnings.warn(
                "Process manager is still running '{}' processes".format(
                    num_procs))

        # Kill any still running processes
        self.assertTrue(self.procman.abort_all(timeout=10.), "Failed to abort all processes")

    def test_launch(self):
        class RanLogger(ProcessMonitorListener):
            def __init__(self):
                self.ran = []

            def on_monitored_process_registered(self, process):
                self.ran.append(process.__class__)

        # Try launching some processes
        for proc_class in TEST_PROCESSES:
            self.publisher.launch(proc_class)

        l = RanLogger()
        with MONITOR.listen(l):
            # Now make them run
            num_ran = 0
            for _ in range(0, 10):
                num_ran += self.subscriber.poll(0.2)
                if num_ran >= len(TEST_PROCESSES):
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
        for c in [ProcessControlSubscriber, ProcessStatusSubscriber, ProcessLaunchSubscriber]:
            subscribers.append(c(connection))
        return subscribers
