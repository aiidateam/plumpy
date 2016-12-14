
from unittest import TestCase
import threading
import time
from plum.process import ProcessState
from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.util import override
from plum.test_utils import DummyProcess, ExceptionProcess, WaitForSignalProcess
from plum.wait_ons import wait_until_stopped, WaitOnState


class EventTracker(ProcessMonitorListener):
    def __init__(self):
        MONITOR.add_monitor_listener(self)
        self.created_called = False
        self.failed_called = False
        self.stopped_called = False

    @override
    def on_monitored_process_created(self, process):
        self.created_called = True

    @override
    def on_monitored_process_stopped(self, process):
        self.stopped_called = True

    @override
    def on_monitored_process_failed(self, pid):
        self.failed_called = True

    def reset(self):
        self.created_called = False
        self.failed_called = False
        self.stopped_called = False


class TestProcessMonitor(TestCase):
    def setUp(self):
        self.assertEqual(len(MONITOR.get_pids()), 0)

    def tearDown(self):
        self.assertEqual(len(MONITOR.get_pids()), 0)

    def test_create_stop(self):
        l = EventTracker()

        self.assertFalse(l.created_called)
        self.assertFalse(l.stopped_called)
        self.assertFalse(l.failed_called)

        DummyProcess.run()

        self.assertTrue(l.created_called)
        self.assertTrue(l.stopped_called)
        self.assertFalse(l.failed_called)

        del l

    def test_create_fail(self):
        l = EventTracker()

        self.assertFalse(l.created_called)
        self.assertFalse(l.stopped_called)
        self.assertFalse(l.failed_called)

        try:
            ExceptionProcess.run()
        except RuntimeError:
            pass
        except BaseException as e:
            print(e.message)

        self.assertTrue(l.created_called)
        self.assertFalse(l.stopped_called)
        self.assertTrue(l.failed_called)

        del l
