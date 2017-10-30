from unittest import TestCase
from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.util import override
from plum.test_utils import DummyProcess, ExceptionProcess
from plum.engine.serial import SerialEngine


class EventTracker(ProcessMonitorListener):
    def __init__(self):
        self.created_called = False
        self.failed_called = False
        self.destroyed_called = False

    def __enter__(self):
        MONITOR.add_monitor_listener(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        MONITOR.remove_monitor_listener(self)

    @override
    def on_monitored_process_created(self, process):
        self.created_called = True

    @override
    def on_monitored_process_destroying(self, process):
        self.destroyed_called = True

    @override
    def on_monitored_process_failed(self, pid):
        self.failed_called = True

    def reset(self):
        self.created_called = False
        self.failed_called = False
        self.destroyed_called = False


class TestProcessMonitor(TestCase):
    def setUp(self):
        self.assertEqual(len(MONITOR.get_pids()), 0)
        self.engine = SerialEngine()

    def tearDown(self):
        self.assertEqual(len(MONITOR.get_pids()), 0)

    def test_create_destroy(self):
        with EventTracker() as l:
            pid = self.engine.submit(DummyProcess).pid

            with self.assertRaises(ValueError):
                MONITOR.get_process(pid)

            self.assertTrue(l.created_called)
            self.assertTrue(l.destroyed_called)
            self.assertFalse(l.failed_called)

    def test_create_fail(self):
        with EventTracker() as l:
            pid = self.engine.submit(ExceptionProcess).pid

            with self.assertRaises(ValueError):
                MONITOR.get_process(pid)

            self.assertTrue(l.created_called)
            self.assertFalse(l.destroyed_called)
            self.assertTrue(l.failed_called)

    def test_get_proecss(self):
        dp = DummyProcess.new_instance()
        pid = dp.pid

        self.assertIs(dp, MONITOR.get_process(pid))
        self.assertEqual(pid, MONITOR.get_pids()[0])
        dp.run_until_complete()
        with self.assertRaises(ValueError):
            MONITOR.get_process(pid)
        self.assertEqual(len(MONITOR.get_pids()), 0)
