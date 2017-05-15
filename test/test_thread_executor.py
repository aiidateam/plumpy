from test.util import TestCase
import time
from plum.process import ProcessState
from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.thread_executor import ThreadExecutor, SchedulingExecutor
from plum.test_utils import DummyProcess, WaitForSignalProcess
from plum.wait_ons import wait_until, wait_until_stopped, WaitOnProcessState


class TestThreadExecutor(TestCase):
    def setUp(self):
        self.assertEqual(len(MONITOR.get_pids()), 0)
        self.executor = ThreadExecutor()

    def tearDown(self):
        self.executor.shutdown()

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
            self.executor.launch(DummyProcess)
            while not t.stopped:
                pass
            self.assertIs(t.proc_class, DummyProcess)

    def test_play(self):
        p = DummyProcess.new()
        self.assertFalse(p.has_finished())

        fut = self.executor.play(p)
        self.assertTrue(fut.wait(timeout=1.))

        self.assertTrue(p.has_terminated())
        self.assertTrue(p.has_finished())

    def test_pause_all(self):
        procs = []

        # Launch a bunch of processes
        for i in range(0, 9):
            procs.append(WaitForSignalProcess.new())
            self.executor.play(procs[-1])

        self.assertTrue(wait_until(procs, ProcessState.WAITING, timeout=5))

        # Check they are all in state we expect
        for p in procs:
            self.assertTrue(p.is_playing())

        # Now try and pause them all
        self.executor.pause_all()

        # Check they are all in state we expect
        for p in procs:
            self.assertEqual(p.state, ProcessState.WAITING)
            self.assertFalse(p.is_playing())

    def test_play_pause_abort(self):
        num_procs = 10
        procs = []
        for i in range(0, num_procs):
            procs.append(WaitForSignalProcess.new())
            self.executor.play(procs[-1])

        # Wait
        self.assertTrue(wait_until(procs, ProcessState.WAITING))

        self.assertEqual(self.executor.pause_all(timeout=3.), num_procs)

    def test_future_pid(self):
        p = DummyProcess.new()
        future = self.executor.play(p)
        self.assertEqual(future.pid, p.pid)

    def test_future_abort(self):
        p = WaitForSignalProcess.new()
        future = self.executor.play(p)

        # Wait
        self.assertTrue(wait_until(p, ProcessState.WAITING))
        self.assertTrue(p.is_playing())

        # Abort
        self.assertTrue(future.abort(timeout=3.))
        self.assertTrue(p.has_aborted())

    def test_future_pause_play(self):
        p = WaitForSignalProcess.new()
        future = self.executor.play(p)

        # Wait
        self.assertTrue(wait_until(p, ProcessState.WAITING))
        self.assertTrue(p.is_playing())

        # Pause
        self.assertTrue(future.pause(timeout=3.))
        self.assertFalse(p.is_playing())

        # Play
        future.play()
        p.continue_()
        self.assertTrue(future.wait(timeout=1.))

    def test_abort(self):
        """
        Test aborting a process through the process manager
        """
        self.assertEqual(self.executor.get_num_processes(), 0)
        proc = WaitForSignalProcess.new()
        future = self.executor.play(proc)
        self.assertTrue(future.abort(timeout=2.))
        self.assertEqual(self.executor.get_num_processes(), 0)

    def test_abort_interrupt(self):
        """
        Test aborting a process through the process manager
        """
        self.assertEqual(self.executor.get_num_processes(), 0)
        proc = WaitForSignalProcess.new()
        # Start a process and make sure it is waiting
        future = self.executor.play(proc)
        wait_until(proc, ProcessState.WAITING)
        # Then interrupt by aborting
        self.assertTrue(future.abort(timeout=2.))
        self.assertEqual(self.executor.get_num_processes(), 0)

    def test_abort_future(self):
        """
        Test aborting a process through the future
        """
        self.assertEqual(self.executor.get_num_processes(), 0)
        proc = WaitForSignalProcess.new()
        future = self.executor.play(proc)
        wait_until(proc, ProcessState.WAITING)
        self.assertTrue(future.abort(timeout=2.))
        self.assertEqual(self.executor.get_num_processes(), 0)

    def test_get_processes(self):
        p = WaitForSignalProcess.new()
        self.executor.play(p)

        procs = self.executor.get_processes()
        self.assertEqual(len(procs), 1)
        self.assertIs(procs[0], p)
        self.assertTrue(p.abort(timeout=2.), "Failed to abort process")


class TestSchedulerExecutor(TestCase):
    def test_simple_queue(self):
        with SchedulingExecutor(max_threads=2) as executor:
            procs = []
            for i in range(10):
                proc = DummyProcess()
                procs.append(proc)
                executor.play(proc)

            self.assertTrue(wait_until(procs, ProcessState.STOPPED, timeout=2.))

    def test_simple_push_over_limit(self):
        with SchedulingExecutor(max_threads=1) as executor:
            p1 = WaitForSignalProcess()
            p2 = WaitForSignalProcess()

            f1 = executor.play(p1)
            self.assertTrue(wait_until(p1, ProcessState.WAITING, timeout=2.))

            # This should push of P1
            f2 = executor.play(p2)
            self.assertTrue(p1.wait(timeout=1.))
            self.assertTrue(wait_until(p2, ProcessState.WAITING, timeout=2.))

            # Now P2 should be paused and P1 running again
            self.assertTrue(p2.wait(timeout=1.))

            # Finish p1
            p1.continue_()
            self.assertTrue(f1.wait(timeout=1.))

            # Now P2 should be running again
            p2.continue_()
            self.assertTrue(f2.wait(timeout=1.))

    def test_push_over_limit(self):
        with SchedulingExecutor(max_threads=2) as executor:
            procs = (WaitForSignalProcess(), WaitForSignalProcess(), WaitForSignalProcess())

            # Play first two
            executor.play(procs[0])
            executor.play(procs[1])

            # Get first two to the waiting state
            self.assertTrue(wait_until((procs[0], procs[1]), ProcessState.WAITING, timeout=2.))

            self.assertTrue(procs[0].is_playing())
            self.assertTrue(procs[1].is_playing())
            self.assertFalse(procs[2].is_playing())

            # Now play third
            executor.play(procs[2])

            # Get third to the waiting state
            self.assertTrue(wait_until(procs[2], ProcessState.WAITING, timeout=2.))

            # Now that it's waiting p2 should be pulled and the other two should be playing
            self.assertTrue(procs[2].wait(timeout=2.))

            # Check the final expected state
            # WARNING: The others could not be playing *yet* because it is too early
            time.sleep(0.1)
            self.assertTrue(procs[1].is_playing())
            self.assertFalse(procs[2].is_playing())
            self.assertTrue(procs[0].is_playing())

    def test_queueing(self):
        with SchedulingExecutor(max_threads=2) as executor:
            procs = (WaitForSignalProcess(), WaitForSignalProcess(),
                     WaitForSignalProcess(), WaitForSignalProcess())

            futs = []
            for proc in procs:
                futs.append(executor.play(proc))

            for proc in procs:
                proc.continue_()

            # Make sure they all finished
            for fut in futs:
                self.assertTrue(fut.wait(timeout=1.))

