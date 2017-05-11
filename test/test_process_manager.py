from test.util import TestCase
import time
from plum.process import ProcessState
from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.thread_executor import ThreadExecutor
from plum.test_utils import DummyProcess, WaitForSignalProcess
from plum.wait_ons import wait_until, wait_until_stopped, WaitOnProcessState


class TestProcessManager(TestCase):
    def setUp(self):
        self.assertEqual(len(MONITOR.get_pids()), 0)
        self.procman = ThreadExecutor()

    def tearDown(self):
        self.procman.abort_all(timeout=10.)
        self.assertEqual(self.procman.get_num_processes(), 0, "Failed to abort all processes")

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

    def test_play(self):
        p = DummyProcess.new()
        self.assertFalse(p.has_finished())

        self.procman.play(p)
        self.assertTrue(p.wait(timeout=1.))

        self.assertTrue(p.has_terminated())
        self.assertTrue(p.has_finished())

    def test_pause_all(self):
        procs = []

        # Launch a bunch of processes
        for i in range(0, 9):
            procs.append(WaitForSignalProcess.new())
            self.procman.play(procs[-1])

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

    def test_play_pause_abort(self):
        num_procs = 10
        procs = []
        for i in range(0, num_procs):
            procs.append(WaitForSignalProcess.new())
            self.procman.play(procs[-1])

        # Wait
        self.assertTrue(wait_until(procs, ProcessState.WAITING))

        self.assertEqual(self.procman.pause_all(timeout=3.), num_procs)

    def test_future_pid(self):
        p = DummyProcess.new()
        future = self.procman.play(p)
        self.assertEqual(future.pid, p.pid)

    def test_future_abort(self):
        p = WaitForSignalProcess.new()
        future = self.procman.play(p)

        # Wait
        self.assertTrue(wait_until(p, ProcessState.WAITING))
        self.assertTrue(p.is_playing())

        # Abort
        self.assertTrue(future.abort(timeout=3.))
        self.assertTrue(p.has_aborted())

    def test_future_pause_play(self):
        p = WaitForSignalProcess.new()
        future = self.procman.play(p)

        # Wait
        self.assertTrue(wait_until(p, ProcessState.WAITING))
        self.assertTrue(p.is_playing())

        # Pause
        self.assertTrue(future.pause(timeout=3.))
        self.assertFalse(p.is_playing())

        # Play
        future.play()
        p.wait(0.1)
        self.assertTrue(p.is_playing())

    def test_abort(self):
        """
        Test aborting a process through the process manager
        """
        self.assertEqual(self.procman.get_num_processes(), 0)
        proc = WaitForSignalProcess.new()
        future = self.procman.play(proc)
        self.assertTrue(self.procman.abort(proc.pid, timeout=2.))
        self.assertEqual(self.procman.get_num_processes(), 0)

    def test_abort_interrupt(self):
        """
        Test aborting a process through the process manager
        """
        self.assertEqual(self.procman.get_num_processes(), 0)
        proc = WaitForSignalProcess.new()
        # Start a process and make sure it is waiting
        future = self.procman.play(proc)
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
        future = self.procman.play(proc)
        wait_until(proc, ProcessState.WAITING)
        self.assertTrue(future.abort(timeout=2.))
        self.assertEqual(self.procman.get_num_processes(), 0)

    def test_get_processes(self):
        p = WaitForSignalProcess.new()
        self.procman.play(p)

        procs = self.procman.get_processes()
        self.assertEqual(len(procs), 1)
        self.assertIs(procs[0], p)
        self.assertTrue(p.abort(timeout=2.), "Failed to abort process")
