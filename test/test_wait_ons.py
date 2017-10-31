from plum import loop_factory
from plum.process import ProcessState
from plum.test_utils import WaitForSignalProcess, DummyProcess
from plum import wait_ons
from .util import TestCase


class TestWaitOnProcessStateEvent(TestCase):
    def setUp(self):
        super(TestWaitOnProcessStateEvent, self).setUp()
        self.loop = loop_factory()

    def test_already_in_state(self):
        p = self.loop.create(DummyProcess)
        wait_for = self.loop.create(wait_ons.WaitOnProcessState, p, ProcessState.CREATED)
        result = self.loop.run_until_complete(wait_for)
        self.assertEqual(result, wait_ons.WaitOnProcessState.STATE_REACHED)

    def test_state_messages(self):
        for state in (ProcessState.RUNNING, ProcessState.STOPPED):
            p = self.loop.create(DummyProcess)

            wait_for = self.loop.create(wait_ons.WaitOnProcessState, p, state)
            result = self.loop.run_until_complete(wait_for)
            self.assertEqual(result, wait_ons.WaitOnProcessState.STATE_REACHED)
            self.assertTrue(p.state, state)

    def test_waiting_state(self):
        p = self.loop.create(WaitForSignalProcess)

        wait_for = self.loop.create(wait_ons.WaitOnProcessState, p, ProcessState.WAITING)
        result = self.loop.run_until_complete(wait_for)

        self.assertEqual(result, wait_ons.WaitOnProcessState.STATE_REACHED)

    def test_wait_until(self):
        p = self.loop.create(WaitForSignalProcess)
        wait_ons.run_until(p, ProcessState.WAITING, self.loop)


# class TestWaitOns(TestCase):
#     def setUp(self):
#         super(TestWaitOns, self).setUp()
#         self.loop = loop_factory()
#
#     def test_wait_on_all(self):
#         waits = [self.loop.create(wait_ons.Checkpoint) for _ in range(20)]
#         wait_on = self.loop.create(wait_ons.WaitOnAll, waits)
#         self.loop.run_until_complete(wait_on)
