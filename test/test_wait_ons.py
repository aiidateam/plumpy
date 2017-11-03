import plum
from plum import wait_ons
from plum.test_utils import WaitForSignalProcess, DummyProcess
from . import util
from .util import TestCase


class TestWaitOnProcessStateEvent(TestCase):
    def setUp(self):
        super(TestWaitOnProcessStateEvent, self).setUp()
        self.loop = plum.new_event_loop()
        plum.set_event_loop(self.loop)

    def test_already_in_state(self):
        proc = DummyProcess()
        wait_for = wait_ons.WaitOnProcessState(proc, plum.ProcessState.CREATED)
        self.loop.run_until_complete(util.MaxTicks(4, wait_for))
        self.assertEqual(wait_for.result(), wait_ons.WaitOnProcessState.STATE_REACHED)

    def test_state_messages(self):
        for state in (plum.ProcessState.RUNNING, plum.ProcessState.STOPPED):
            proc = DummyProcess()
            proc.play()

            wait_for = wait_ons.WaitOnProcessState(proc, state)
            self.loop.run_until_complete(util.MaxTicks(4, wait_for))

            self.assertEqual(wait_for.result(), wait_ons.WaitOnProcessState.STATE_REACHED)
            self.assertTrue(proc.state, state)

    def test_waiting_state(self):
        proc = WaitForSignalProcess()
        proc.play()
        proc.continue_()
        wait_for = wait_ons.WaitOnProcessState(proc, plum.ProcessState.WAITING)
        self.loop.run_until_complete(util.MaxTicks(4, wait_for))
        self.assertEqual(wait_for.result(), wait_ons.WaitOnProcessState.STATE_REACHED)

    def test_wait_until(self):
        p = self.loop.create(WaitForSignalProcess)
        p.play()
        p.continue_()
        wait_ons.run_until(p, plum.ProcessState.WAITING, self.loop)
        self.assertTrue(p.state, plum.ProcessState.WAITING)


