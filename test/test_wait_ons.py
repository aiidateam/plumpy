from concurrent.futures import ThreadPoolExecutor as PythonThreadPoolExecutor
import time
from util import TestCase
from plum.process import ProcessState
from plum.exceptions import Interrupted
from plum.wait_ons import WaitOnProcessState, wait_until
from plum.test_utils import WaitForSignalProcess, DummyProcess
from plum.thread_executor import ThreadExecutor


class TestWaitOnProcessStateEvent(TestCase):
    def setUp(self):
        super(TestWaitOnProcessStateEvent, self).setUp()
        self.executor = ThreadExecutor()

    def tearDown(self):
        self.executor.abort_all(timeout=10.)
        self.assertEqual(self.executor.get_num_processes(), 0, "Failed to abort all processes")

    def test_already_in_state(self):
        p = DummyProcess.new()
        self.assertTrue(WaitOnProcessState(p, ProcessState.CREATED).wait(timeout=2.))

    def test_state_messages(self):
        tp = PythonThreadPoolExecutor(max_workers=1)
        for state in (ProcessState.RUNNING, ProcessState.STOPPED):
            p = DummyProcess.new()
            waiton = WaitOnProcessState(p, state)
            future = tp.submit(waiton.wait)
            while not future.running():
                pass

            p.play()
            self.assertTrue(future.result(timeout=2.))

    def test_waiting_state(self):
        tp = PythonThreadPoolExecutor(max_workers=1)

        p = WaitForSignalProcess.new()
        waiton = WaitOnProcessState(p, ProcessState.WAITING)
        future = tp.submit(waiton.wait)

        self.executor.play(p)
        self.assertTrue(future.result(timeout=2.))
        self.assertTrue(p.abort(timeout=2.))

    def test_interrupt(self):
        tp = PythonThreadPoolExecutor(max_workers=1)

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
        self.executor.play(p)
        self.assertTrue(wait_until(p, ProcessState.WAITING, timeout=1.))
