from util import TestCase
from plum.process import ProcessState
from plum.wait_ons import WaitOnState
from plum.test_utils import WaitForSignalProcess, DummyProcess
from plum.process_manager import ProcessManager


class TestWaitOnState(TestCase):
    def setUp(self):
        super(TestWaitOnState, self).setUp()
        self.manager = ProcessManager()

    def test_state_messages(self):
        p = WaitForSignalProcess.new()

        # Create ones for each state
        waits = {state: WaitOnState(p, state)
                 for state in [
                     ProcessState.RUNNING,
                     ProcessState.WAITING,
                     ProcessState.STOPPED]}

        for wait in waits.itervalues():
            self.assertFalse(wait.is_done())

        future = self.manager.start(p)
        self.assertTrue(waits[ProcessState.RUNNING].wait(1))
        self.assertTrue(waits[ProcessState.WAITING].wait(1))
        self.assertFalse(waits[ProcessState.STOPPED].wait(1))
        p.continue_()
        self.assertTrue(waits[ProcessState.STOPPED].wait(1))
        assert future.wait(1.)

    def test_interrupt(self):
        p = DummyProcess.new()
        w = WaitOnState(p, ProcessState.STOPPED)
        self.assertFalse(w.wait(0.2))
        w.interrupt()
        future = self.manager.start(p)
        self.assertFalse(w.wait(0.2))
        assert future.wait(1.)
