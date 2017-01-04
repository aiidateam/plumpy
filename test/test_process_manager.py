
from unittest import TestCase
from plum.process import ProcessState
from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.process_manager import ProcessManager
from plum.test_utils import DummyProcess, WaitForSignalProcess
from plum.wait_ons import wait_until, wait_until_stopped


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
        p = DummyProcess.new_instance()
        self.assertFalse(p.has_finished())
        self.manager.start(p)
        wait_until_stopped(p, 1)
        self.assertTrue(p.has_finished())

    def test_pause_all(self):
        procs = []

        # Launch a bunch of processes
        for i in range(0, 9):
            procs.append(WaitForSignalProcess.new_instance())
            self.manager.start(procs[-1])

        self.assertTrue(wait_until(procs, ProcessState.WAITING, timeout=5))

        # Check they are all in state we expect
        for p in procs:
            self.assertTrue(p.is_executing())

        # Now try and pause them all
        self.manager.pause_all()

        # Check they are all in state we expect
        for p in procs:
            self.assertEqual(p.state, ProcessState.WAITING)
            self.assertFalse(p.is_executing())

    def test_play_all(self):
        procs = []

        # Launch a bunch of processes
        for i in range(0, 9):
            procs.append(WaitForSignalProcess.new_instance())
            self.manager.start(procs[-1])

        wait_until(procs, ProcessState.WAITING, timeout=1)

        # Check they are all in state we expect
        for p in procs:
            self.assertTrue(p.is_executing(), "state '{}'".format(p.state))

        # Now try and pause them all
        self.manager.pause_all()

        # Check they are all in state we expect
        for p in procs:
            self.assertEqual(p.state, ProcessState.WAITING)
            self.assertFalse(p.is_executing())

        self.manager.play_all()

        for p in procs:
            p.continue_()
        wait_until_stopped(procs)

        for p in procs:
            self.assertEqual(p.state, ProcessState.STOPPED)
            self.assertFalse(p.is_executing())

    def test_play_pause_abort(self):
        procs = []
        for i in range(0, 10):
            procs.append(WaitForSignalProcess.new_instance())
            self.manager.start(procs[-1])
        self.assertTrue(wait_until(procs, ProcessState.WAITING))
        self.assertTrue(self.manager.pause_all(timeout=2))
        self.assertTrue(self.manager.abort_all(timeout=2))
