import apricotpy
from plum import loop_factory
from plum.wait import WaitOn
from .util import TestCase


class _DummyWait(WaitOn):
    def __init__(self, value):
        super(_DummyWait, self).__init__()
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
        loop = loop_factory()

        w = ~loop.create_inserted(_DummyWait, 5)
        saved_state = apricotpy.persistable.Bundle(w)
        ~w.remove()

        w_ = saved_state.unbundle(loop)
        self.assertEqual(w._value, w_._value)
