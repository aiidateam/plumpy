
import time
import pika
import threading
from plum.process_manager import ProcessManager
from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.rmq import TaskRunner, TaskController, ProcessController, \
    action_decode, action_encode
from plum.test_utils import TEST_PROCESSES, DummyProcess, WaitForSignalProcess
from plum.wait_ons import wait_until
from plum.process import ProcessState
from util import TestCase


class Out(object):
    def __init__(self):
        self.runner = None
        self.is_set = threading.Event()


class TestTaskController(TestCase):
    def _start_runner(self, out=None):
        connection = pika.BlockingConnection()
        runner = TaskRunner(connection)
        if out is not None:
            out.runner = runner
            out.is_set.set()
        runner.start()
        connection.close()

    def _launch_runner(self):
        o = Out()
        thread = threading.Thread(target=self._start_runner, args=[o])
        thread.start()
        o.is_set.wait()
        return thread, o.runner

    def setUp(self):
        super(TestTaskController, self).setUp()

        self._thread, self._runner = self._launch_runner()
        connection = pika.BlockingConnection()
        self.sender = TaskController(connection)

    def tearDown(self):
        self._runner.stop()
        self.safe_join(self._thread, 2)
        super(TestTaskController, self).tearDown()

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

            time.sleep(1)
            while len(MONITOR.get_pids()) > 0:
                pass

        self.assertListEqual(TEST_PROCESSES, l.ran)


class TestProcessController(TestCase):
    def setUp(self):
        super(TestProcessController, self).setUp()
        self._connection = pika.BlockingConnection()

        self.exchange = '{}.task_control'.format(self.__class__)

        self.channel = self._connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, type='fanout')

        self.manager = ProcessManager()
        self.controller = ProcessController(
            self._connection, exchange=self.exchange,
            process_manager=self.manager)

    def tearDown(self):
        super(TestProcessController, self).tearDown()
        self._connection.close()

    def test_pause(self):
        # Create the process and wait until it is waiting
        p = WaitForSignalProcess.new_instance()
        self.manager.start(p)
        wait_until(p, ProcessState.WAITING)
        self.assertTrue(p.is_executing())

        # Send a message asking the process to pause
        self.channel.basic_publish(
            exchange=self.exchange, routing_key='',
            body=action_encode({'pid': p.pid, 'intent': 'pause'}))
        self.controller.poll(time_limit=1)

        self.assertFalse(p.is_executing())

    def test_pause_play(self):
        def test_pause(self):
            # Create the process and wait until it is waiting
            p = WaitForSignalProcess.new_instance()
            self.manager.start(p)
            wait_until(p, ProcessState.WAITING)
            self.assertTrue(p.is_executing())

            # Send a message asking the process to pause
            self.channel.basic_publish(
                exchange=self.exchange, routing_key='',
                body=action_encode({'pid': p.pid, 'intent': 'pause'}))
            self.controller.poll(time_limit=1)

            self.assertFalse(p.is_executing())

            # Now ask it to continue
            self.channel.basic_publish(
                exchange=self.exchange, routing_key='',
                body=action_encode({'pid': p.pid, 'intent': 'play'}))
            self.controller.poll(time_limit=1)

            self.assertTrue(p.is_executing())

    def test_abort(self):
        # Create the process and wait until it is waiting
        p = WaitForSignalProcess.new_instance()
        self.manager.start(p)
        wait_until(p, ProcessState.WAITING)
        self.assertTrue(p.is_executing())

        # Send a message asking the process to abort
        self.channel.basic_publish(
            exchange=self.exchange, routing_key='',
            body=action_encode({'pid': p.pid, 'intent': 'abort'}))
        self.controller.poll(time_limit=1)

        wait_until(p, ProcessState.STOPPED, 1)
        self.assertTrue(p.has_aborted())

