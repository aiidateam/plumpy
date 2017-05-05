import unittest
from plum.wait import WaitOn
from plum.persistence import Bundle
from util import TestCase


class _DummyWait(WaitOn):
    def __init__(self, value):
        super(_DummyWait, self).__init__()
        self._value = value

    def wait(self, timeout=None):
        return True

    def interrupt(self):
        pass

    def save_instance_state(self, out_state):
        super(_DummyWait, self).save_instance_state(out_state)
        out_state['value'] = self._value

    def load_instance_state(self, saved_state):
        super(_DummyWait, self).load_instance_state(saved_state)
        self._value = saved_state['value']


class MyTestCase(TestCase):
    def test_save_load(self):
        """
        Basic test to check saving instance state and reloading
        """
        w = _DummyWait(5)
        b = Bundle()
        w.save_instance_state(b)
        w_ = _DummyWait.create_from(b)

        self.assertEqual(w._value, w_._value)
