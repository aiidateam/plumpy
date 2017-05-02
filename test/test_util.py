from util import TestCase
from plum.util import Savable
from plum.persistence import Bundle


class TestSavable(TestCase):
    def test_basic(self):
        class A(Savable):
            def __init__(self, a):
                self.value = a

            def save_instance_state(self, out_state):
                out_state['value'] = self.value

            def load_instance_state(self, saved_state):
                self.value = saved_state['value']

        a = A(5)
        self.assertEqual(a.value, 5)
        state = Bundle()
        a.save_instance_state(state)

        a2 = A.create_from(state)
        self.assertEqual(a.value, a2.value)


