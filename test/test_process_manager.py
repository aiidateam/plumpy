from test.util import TestCase
import time
from plum.process import ProcessState
from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.process_manager import ProcessManager
from plum.test_utils import DummyProcess, WaitForSignalProcess
from plum.wait_ons import wait_until, wait_until_stopped, WaitOnProcessState, WaitRegion


class TestProcessManager(TestCase):
    def setUp(self):
        self.assertEqual(len(MONITOR.get_pids()), 0)
        self.procman = ProcessManager()

    def tearDown(self):
        self.assertTrue(self.procman.abort_all(timeout=10.), "Failed to abort all processes")

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
            self.procman.launch(DummyProcess)
            while not t.stopped:
                pass
            self.assertIs(t.proc_class, DummyProcess)

    def test_start(self):
        p = DummyProcess.new()
        self.assertFalse(p.has_finished())
        self.procman.start(p)
        wait_until_stopped(p, 1)
        self.assertTrue(p.has_finished())

    def test_pause_all(self):
        procs = []

        # Launch a bunch of processes
        for i in range(0, 9):
            procs.append(WaitForSignalProcess.new())
            self.procman.start(procs[-1])

        self.assertTrue(wait_until(procs, ProcessState.WAITING, timeout=5))

        # Check they are all in state we expect
        for p in procs:
            self.assertTrue(p.is_playing())

        # Now try and pause them all
        self.procman.pause_all()

        # Check they are all in state we expect
        for p in procs:
            self.assertEqual(p.state, ProcessState.WAITING)
            self.assertFalse(p.is_playing())

    def test_play_all(self):
        procs = []

        # Launch a bunch of processes
        for i in range(0, 9):
            procs.append(WaitForSignalProcess.new())
            self.procman.start(procs[-1])

        wait_until(procs, ProcessState.WAITING, timeout=1)

        # Check they are all in state we expect
        for p in procs:
            self.assertTrue(p.is_playing(), "state '{}'".format(p.state))

        # Now try and pause them all
        self.procman.pause_all()

        # Check they are all in state we expect
        for p in procs:
            self.assertEqual(p.state, ProcessState.WAITING)
            self.assertFalse(p.is_playing())

        # Play them all
        self.procman.play_all()
        for p in procs:
            p.continue_()
        self.assertTrue(wait_until_stopped(procs, timeout=5.), "Not all processes stopped")

        # Check they all finished
        for p in procs:
            self.assertTrue(p.has_finished())

    def test_play_pause_abort(self):
        procs = []
        for i in range(0, 10):
            procs.append(WaitForSignalProcess.new())
            self.procman.start(procs[-1])
        self.assertTrue(wait_until(procs, ProcessState.WAITING))
        self.assertTrue(self.procman.pause_all(timeout=5))
        self.assertTrue(self.procman.abort_all(timeout=5))

    def test_future_pid(self):
        p = DummyProcess.new()
        future = self.procman.start(p)
        self.assertEqual(future.pid, p.pid)

    def test_future_abort(self):
        p = WaitForSignalProcess.new()

        with WaitRegion(WaitOnProcessState(p, ProcessState.WAITING), timeout=2):
            future = self.procman.start(p)

        self.assertTrue(p.is_playing())
        self.assertTrue(future.abort(timeout=3.))
        self.assertTrue(p.has_aborted())

    def test_future_pause_play(self):
        p = WaitForSignalProcess.new()

        # Run the process
        with WaitRegion(WaitOnProcessState(p, ProcessState.WAITING), timeout=2):
            future = self.procman.start(p)
        self.assertTrue(p.is_playing())

        # Pause it
        self.assertTrue(future.pause(timeout=3.))
        self.assertFalse(p.is_playing())

        # Play it
        future.play()
        p.wait(1.)
        self.assertTrue(p.is_playing())

    def test_abort(self):
        """
        Test aborting a process through the process manager
        """
        self.assertEqual(self.procman.get_num_processes(), 0)
        proc = WaitForSignalProcess.new()
        future = self.procman.start(proc)
        self.assertTrue(future.abort(timeout=2.))
        self.assertEqual(self.procman.get_num_processes(), 0)

    def test_abort_interrupt(self):
        """
        Test aborting a process through the process manager
        """
        self.assertEqual(self.procman.get_num_processes(), 0)
        proc = WaitForSignalProcess.new()
        # Start a process and make sure it is waiting
        future = self.procman.start(proc)
        wait_until(proc, ProcessState.WAITING)
        # Then interrupt by aborting
        self.assertTrue(future.abort(timeout=2.))
        self.assertEqual(self.procman.get_num_processes(), 0)

    def test_abort_future(self):
        """
        Test aborting a process through the future
        """
        self.assertEqual(self.procman.get_num_processes(), 0)
        proc = WaitForSignalProcess.new()
        future = self.procman.start(proc)
        wait_until(proc, ProcessState.WAITING)
        self.assertTrue(future.abort(timeout=2.))
        self.assertEqual(self.procman.get_num_processes(), 0)

    def test_get_processes(self):
        p = WaitForSignalProcess.new()
        with WaitRegion(WaitOnProcessState(p, ProcessState.RUNNING), timeout=2):
            self.procman.start(p)
        procs = self.procman.get_processes()
        self.assertEqual(len(procs), 1)
        self.assertIs(procs[0], p)
        self.assertTrue(p.abort(timeout=2.), "Failed to abort process")
