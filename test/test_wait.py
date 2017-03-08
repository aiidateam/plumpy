import unittest
from plum.wait import WaitOn
from plum.persistence import Bundle


class _DummyWait(WaitOn):
    def init(self, value):
        super(_DummyWait, self).init()
        self._value = value

    def save_instance_state(self, out_state):
        super(_DummyWait, self).save_instance_state(out_state)
        out_state['value'] = self._value

    def load_instance_state(self, bundle):
        super(_DummyWait, self).load_instance_state(bundle)
        self._value = bundle['value']


class MyTestCase(unittest.TestCase):
    def test_save_load(self):
        """
        Basic test to check saving instance state and reloading
        """
        w = _DummyWait(5)
        b = Bundle()
        w.save_instance_state(b)
        w_ = _DummyWait.create_from(b)

        self.assertEqual(w._value, w_._value)
