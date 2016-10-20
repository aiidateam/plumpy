
from unittest import TestCase
import threading
import time
from plum.process import ProcessState
from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.util import override
from plum.test_utils import DummyProcess, ExceptionProcess, WaitForSignalProcess
from plum.wait_ons import wait_until_destroyed, WaitOnState


class EventTracker(ProcessMonitorListener):
    def __init__(self):
        MONITOR.add_monitor_listener(self)
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

    def reset(self):
        self.created_called = False
        self.failed_called = False
        self.destroyed_called = False

    def __del__(self):
        MONITOR.remove_monitor_listener(self)


class TestProcessMonitor(TestCase):
    def setUp(self):
        self.assertEqual(len(MONITOR.get_pids()), 0)

    def tearDown(self):
        self.assertEqual(len(MONITOR.get_pids()), 0)

    def test_create_destroy(self):
        l = EventTracker()

        self.assertFalse(l.created_called)
        self.assertFalse(l.destroyed_called)
        self.assertFalse(l.failed_called)

        DummyProcess.run()

        self.assertTrue(l.created_called)
        self.assertTrue(l.destroyed_called)
        self.assertFalse(l.failed_called)

        del l

    def test_create_fail(self):
        l = EventTracker()

        self.assertFalse(l.created_called)
        self.assertFalse(l.destroyed_called)
        self.assertFalse(l.failed_called)

        try:
            ExceptionProcess.run()
        except:
            pass

        self.assertTrue(l.created_called)
        self.assertFalse(l.destroyed_called)
        self.assertTrue(l.failed_called)

        del l

    # def test_run_through(self):
    #     p = WaitForSignalProcess.new_instance()
    #     pid = p.pid
    #
    #     # Start the process
    #     t = threading.Thread(target=p.start)
    #     t.start()
    #
    #     # Wait until it is running
    #     WaitOnState(p, ProcessState.RUNNING).wait()
    #
    #     # Check that the process monitor knows about it
    #     self.assertIsNotNone(MONITOR.get_process(pid))
    #     self.assertEqual(pid, MONITOR.get_pids()[0])
    #
    #     # Tell the process to continue
    #     p.signal()
    #     wait_until_destroyed(p)
    #     self.assertEqual(p.state, ProcessState.DESTROYED)
    #
    #     t.join()
    #
    #     # Check that the process monitor knows it's done
    #     with self.assertRaises(ValueError):
    #         MONITOR.get_process(pid)
    #     self.assertEqual(len(MONITOR.get_pids()), 0)

