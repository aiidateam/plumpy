
from unittest import TestCase
from plum.process_monitor import monitor, ProcessMonitor, ProcessMonitorListener
from plum.util import override
from plum.test_utils import DummyProcess


class EventTracker(ProcessMonitorListener):
    def __init__(self):
        monitor.add_monitor_listener(self)
        self.created_called = False
        self.failed_called = False
        self.destroyed_called = False

    @override
    def on_monitored_process_created(self, process):
        self.created_called = True

    @override
    def on_monitored_process_destroying(self, process):
        self.destroyed_called = True

    @override
    def on_monitored_process_failed(self, pid):
        self.failed_called = True

    def __del__(self):
        monitor.remove_monitor_listener(self)


class TestProcessMonitor(TestCase):
    def test_create_destroy(self):
        l = EventTracker()

        d = DummyProcess()
        d.perform_create()
        self.assertTrue(l.created_called)
        d.perform_destroy()
        self.assertTrue(l.destroyed_called)
        del d
        del l
