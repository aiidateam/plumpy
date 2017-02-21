
import pika
import pika.exceptions
import threading
import uuid
import json
from plum.process_manager import ProcessManager
from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.rmq import TaskRunner, TaskController, ProcessController, \
    StatusProvider, SubscriberThread, action_decode, action_encode
from plum.test_utils import TEST_PROCESSES, DummyProcess, WaitForSignalProcess
from plum.wait_ons import wait_until
from plum.process import ProcessState
from util import TestCase


class Out(object):
    def __init__(self):
        self.runner = None
        self.is_set = threading.Event()


class TestTaskControllerAndRunner(TestCase):
    def setUp(self):
        super(TestTaskControllerAndRunner, self).setUp()

        try:
            connection = pika.BlockingConnection()
        except pika.exceptions.ConnectionClosed:
            self.fail("Couldn't open connection.  Make sure rmq server is running")

        queue = "{}.{}.tasks".format(self.__class__, uuid.uuid4())
        self.sender = TaskController(connection, queue=queue)
        self.runner = TaskRunner(connection, queue=queue)

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
                self.sender.send(ProcClass)

            # Now make them run
            num_ran = 0
            while num_ran < len(TEST_PROCESSES):
                num_ran += self.runner.poll(0.2)
            self.assertEqual(num_ran, len(TEST_PROCESSES))

        self.assertListEqual(TEST_PROCESSES, l.ran)


class TestProcessController(TestCase):
    def setUp(self):
        super(TestProcessController, self).setUp()
        try:
            self._connection = pika.BlockingConnection()
        except pika.exceptions.ConnectionClosed:
            self.fail("Couldn't open connection.  Make sure rmq server is running")

        self.exchange = '{}.{}.task_control'.format(
            self.__class__, uuid.uuid4())

        self.channel = self._connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, type='fanout')

        self.manager = ProcessManager()
        self.controller = ProcessController(
            self._connection, exchange=self.exchange,
            process_manager=self.manager)

    def tearDown(self):
        self.manager.shutdown()
        super(TestProcessController, self).tearDown()
        self.channel.close()
        self._connection.close()

    def test_pause(self):
        # Create the process and wait until it is waiting
        p = WaitForSignalProcess.new()
        self.manager.start(p)
        wait_until(p, ProcessState.WAITING)
        self.assertTrue(p.is_playing())

        # Send a message asking the process to pause
        self.channel.basic_publish(
            exchange=self.exchange, routing_key='',
            body=action_encode({'pid': p.pid, 'intent': 'pause'}))
        self.controller.poll(time_limit=1)

        self.assertFalse(p.is_playing())
        self.assertTrue(self.manager.abort(p.pid, timeout=10))

    def test_pause_play(self):
        def test_pause(self):
            # Create the process and wait until it is waiting
            p = WaitForSignalProcess.new()
            self.manager.play(p)
            self.assertTrue(wait_until(p, ProcessState.WAITING, 1))
            self.assertTrue(p.is_playing())

            # Send a message asking the process to pause
            self.channel.basic_publish(
                exchange=self.exchange, routing_key='',
                body=action_encode({'pid': p.pid, 'intent': 'pause'}))
            self.controller.poll_response(time_limit=1)

            self.assertFalse(p.is_playing())

            # Now ask it to continue
            self.channel.basic_publish(
                exchange=self.exchange, routing_key='',
                body=action_encode({'pid': p.pid, 'intent': 'play'}))
            self.controller.poll_response(time_limit=1)

            self.assertTrue(p.is_playing())
            self.assertTrue(self.manager.abort(p.pid, timeout=10))

    def test_abort(self):
        # Create the process and wait until it is waiting
        p = WaitForSignalProcess.new()
        self.manager.start(p)
        wait_until(p, ProcessState.WAITING)
        self.assertTrue(p.is_playing())

        # Send a message asking the process to abort
        self.channel.basic_publish(
            exchange=self.exchange, routing_key='',
            body=action_encode({'pid': p.pid, 'intent': 'abort'}))
        self.controller.poll(time_limit=1)

        self.assertTrue(wait_until(p, ProcessState.STOPPED, 10),
                        "Process did not stop before timeout")
        self.assertTrue(p.has_aborted())
        self.manager.shutdown()


class TestRmqThread(TestCase):
    def test_start_stop(self):
        for c in [ProcessController, StatusProvider, TaskRunner]:
            t = SubscriberThread(c)
            t.set_poll_time(0.0)
            t.start()
            self.assertTrue(t.wait_till_started(1), "Subscriber thread failed to start")
            t.stop()
            t.join(2)
            self.assertFalse(t.is_alive())