
import time
import pika
import threading
from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.rmq import TaskRunner, TaskSender
from plum.test_utils import TEST_PROCESSES, DummyProcess
from util import TestCase


class Out(object):
    def __init__(self):
        self.runner = None
        self.is_set = threading.Event()


class TestTaskSender(TestCase):
    def _start_runner(self, out=None):
        connection = pika.BlockingConnection()
        runner = TaskRunner(connection)
        if out is not None:
            out.runner = runner
            out.is_set.set()
        runner.start()

    def _launch_runner(self):
        o = Out()
        thread = threading.Thread(target=self._start_runner, args=[o])
        thread.start()
        o.is_set.wait()
        return thread, o.runner

    def setUp(self):
        super(TestTaskSender, self).setUp()

        self._thread, self._runner = self._launch_runner()
        connection = pika.BlockingConnection()
        self.sender = TaskSender(connection)

    def tearDown(self):
        self._runner.stop()
        self.safe_join(self._thread, 2)
        super(TestTaskSender, self).tearDown()

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

