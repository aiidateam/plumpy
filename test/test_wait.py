from plum import loop_factory
from plum.wait import WaitOn
from plum.persistence import Bundle
from util import TestCase


class _DummyWait(WaitOn):
    def __init__(self, loop, value):
        super(_DummyWait, self).__init__(loop)
        self._value = value
        self.set_result(self._value)

    def save_instance_state(self, out_state):
        super(_DummyWait, self).save_instance_state(out_state)
        out_state['value'] = self._value

    def load_instance_state(self, loop, saved_state, *args):
        super(_DummyWait, self).load_instance_state(loop, saved_state, *args)
        self._value = saved_state['value']


class TestWaitOn(TestCase):
    def test_save_load(self):
        """
        Basic test to check saving instance state and reloading
        """
        loop = loop_factory()

        w = loop.create(_DummyWait, 5)
        saved_state = Bundle()
        w.save_instance_state(saved_state)
        w_ = loop.create(_DummyWait, saved_state)

        self.assertEqual(w._value, w_._value)
