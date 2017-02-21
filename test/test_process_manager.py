
from unittest import TestCase
import time
from plum.process import ProcessState
from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.process_manager import ProcessManager
from plum.test_utils import DummyProcess, WaitForSignalProcess
from plum.wait_ons import wait_until, wait_until_stopped, WaitOnState, WaitRegion


class TestProcessManager(TestCase):
    def setUp(self):
        self.assertEqual(len(MONITOR.get_pids()), 0)
        self.manager = ProcessManager()

    def tearDown(self):
        self.manager.shutdown()
        self.assertEqual(len(MONITOR.get_pids()), 0)

    def test_launch_simple(self):
        class Tester(ProcessMonitorListener):
            def __init__(self):
                self.proc_class = None
                self.stopped = False

            def on_monitored_process_registered(self, process):
                self.proc_class = process.__class__

            def on_monitored_process_stopped(self, process):
                self.stopped = True

        t = Tester()
        with MONITOR.listen(t):
            self.manager.launch(DummyProcess)
            while not t.stopped:
                pass
            self.assertIs(t.proc_class, DummyProcess)

    def test_start(self):
        p = DummyProcess.new()
        self.assertFalse(p.has_finished())
        self.manager.start(p)
        wait_until_stopped(p, 1)
        self.assertTrue(p.has_finished())

    def test_pause_all(self):
        procs = []

        # Launch a bunch of processes
        for i in range(0, 9):
            procs.append(WaitForSignalProcess.new())
            self.manager.start(procs[-1])

        self.assertTrue(wait_until(procs, ProcessState.WAITING, timeout=5))

        # Check they are all in state we expect
        for p in procs:
            self.assertTrue(p.is_playing())

        # Now try and pause them all
        self.manager.pause_all()

        # Check they are all in state we expect
        for p in procs:
            self.assertEqual(p.state, ProcessState.WAITING)
            self.assertFalse(p.is_playing())

    def test_play_all(self):
        procs = []

        # Launch a bunch of processes
        for i in range(0, 9):
            procs.append(WaitForSignalProcess.new())
            self.manager.start(procs[-1])

        wait_until(procs, ProcessState.WAITING, timeout=1)

        # Check they are all in state we expect
        for p in procs:
            self.assertTrue(p.is_playing(), "state '{}'".format(p.state))

        # Now try and pause them all
        self.manager.pause_all()

        # Check they are all in state we expect
        for p in procs:
            self.assertEqual(p.state, ProcessState.WAITING)
            self.assertFalse(p.is_playing())

        self.manager.play_all()

        for p in procs:
            p.continue_()
        wait_until_stopped(procs)

        for p in procs:
            self.assertEqual(p.state, ProcessState.STOPPED)
            self.manager.shutdown()
            self.assertFalse(p.is_playing())

    def test_play_pause_abort(self):
        procs = []
        for i in range(0, 10):
            procs.append(WaitForSignalProcess.new())
            self.manager.start(procs[-1])
        self.assertTrue(wait_until(procs, ProcessState.WAITING))
        self.assertTrue(self.manager.pause_all(timeout=2))
        self.assertTrue(self.manager.abort_all(timeout=2))

    def test_future_pid(self):
        p = DummyProcess.new()
        future = self.manager.start(p)
        self.assertEqual(future.pid, p.pid)

    def test_future_abort(self):
        p = WaitForSignalProcess.new()

        with WaitRegion(WaitOnState(p, ProcessState.RUNNING), timeout=2):
            future = self.manager.start(p)

        self.assertTrue(p.is_playing())
        self.assertTrue(future.abort(timeout=2))
        self.assertTrue(p.has_aborted())

    def test_future_pause_play(self):
        p = WaitForSignalProcess.new()

        # Run the process
        with WaitRegion(WaitOnState(p, ProcessState.WAITING), timeout=2):
            future = self.manager.start(p)
        self.assertTrue(p.is_playing())

        # Pause it
        self.assertTrue(future.pause(timeout=2))
        self.assertFalse(p.is_playing())

        # Play it
        future.play()
        time.sleep(1)
        self.assertTrue(p.is_playing())
