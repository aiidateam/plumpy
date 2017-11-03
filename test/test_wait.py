import plum
from plum.wait import WaitOn
from .util import TestCase


class _DummyWait(WaitOn):
    def __init__(self, value, loop=None):
        super(_DummyWait, self).__init__(loop)
        self._value = value
        self.set_result(self._value)

    def save_instance_state(self, out_state):
        super(_DummyWait, self).save_instance_state(out_state)
        out_state['value'] = self._value

    def load_instance_state(self, saved_state):
        super(_DummyWait, self).load_instance_state(saved_state)

        self._value = saved_state['value']


class TestWaitOn(TestCase):
    def test_save_load(self):
        """
        Basic test to check saving instance state and reloading
        """
        loop = plum.new_event_loop()

        w = loop.create(_DummyWait, 5)
        saved_state = plum.Bundle(w)

        w_ = saved_state.unbundle(loop)
        self.assertEqual(w._value, w_._value)
