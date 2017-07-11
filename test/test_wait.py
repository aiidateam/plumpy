import unittest
from plum.loop.event_loop import BaseEventLoop
from plum.wait import WaitOn
from plum.persistence import Bundle
from util import TestCase


class _DummyWait(WaitOn):
    def __init__(self, value, loop):
        super(_DummyWait, self).__init__(loop)
        self._value = value
        self.future().set_result(self._value)

    def save_instance_state(self, out_state):
        super(_DummyWait, self).save_instance_state(out_state)
        out_state['value'] = self._value

    def load_instance_state(self, saved_state, loop):
        super(_DummyWait, self).load_instance_state(saved_state, loop)
        self._value = saved_state['value']


class MyTestCase(TestCase):
    def test_save_load(self):
        """
        Basic test to check saving instance state and reloading
        """
        loop = BaseEventLoop()

        w = _DummyWait(5, loop)
        b = Bundle()
        w.save_instance_state(b)
        w_ = _DummyWait.create_from(b, loop)

        self.assertEqual(w._value, w_._value)
