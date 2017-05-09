from concurrent.futures import ThreadPoolExecutor
import time
from util import TestCase
from plum.process import ProcessState
from plum.exceptions import Interrupted
from plum.wait_ons import WaitOnProcessState, wait_until
from plum.test_utils import WaitForSignalProcess, DummyProcess
from plum.process_manager import ProcessManager


class TestWaitOnProcessStateEvent(TestCase):
    def setUp(self):
        super(TestWaitOnProcessStateEvent, self).setUp()
        self.procman = ProcessManager()

    def tearDown(self):
        self.procman.abort_all(timeout=10.)
        self.assertEqual(self.procman.get_num_processes(), 0, "Failed to abort all processes")

    def test_already_in_state(self):
        p = DummyProcess.new()
        self.assertTrue(WaitOnProcessState(p, ProcessState.CREATED).wait(timeout=2.))

    def test_state_messages(self):
        tp = ThreadPoolExecutor(max_workers=1)
        for state in (ProcessState.RUNNING, ProcessState.STOPPED):
            p = DummyProcess.new()
            waiton = WaitOnProcessState(p, state)
            future = tp.submit(waiton.wait)
            while not future.running():
                pass

            p.play()
            self.assertTrue(future.result(timeout=2.))

    def test_waiting_state(self):
        tp = ThreadPoolExecutor(max_workers=1)

        p = WaitForSignalProcess.new()
        waiton = WaitOnProcessState(p, ProcessState.WAITING)
        future = tp.submit(waiton.wait)

        self.procman.play(p)
        self.assertTrue(future.result(timeout=2.))
        self.assertTrue(p.abort(timeout=2.))

    def test_interrupt(self):
        tp = ThreadPoolExecutor(max_workers=1)

        p = DummyProcess.new()
        waiton = WaitOnProcessState(p, ProcessState.STOPPED)
        future = tp.submit(waiton.wait)
        while not future.running():
            pass

        with self.assertRaises(Interrupted):
            waiton.interrupt()
            future.result(timeout=2.)

    def test_interrupt_not_waiting(self):
        """
        If you interrupt when it's not waiting then nothing happens.
        """
        p = DummyProcess.new()
        waiton = WaitOnProcessState(p, ProcessState.STOPPED)
        waiton.interrupt()

    def test_wait_until(self):
        p = WaitForSignalProcess.new()
        self.procman.play(p)
        self.assertTrue(wait_until(p, ProcessState.WAITING, timeout=1.))
