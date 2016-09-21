import unittest
from plum.wait import validate_callback_func
from plum.process import Process
from plum.util import override


class MyTestCase(unittest.TestCase):
    def test_validate_callback_func(self):
        def callback(self, wait_on):
            pass

        # Check error on non-member
        with self.assertRaises(AssertionError):
            validate_callback_func(callback)

        class P(Process):
            def bad_callback1(self):
                pass

            def bad_callback2(self, a, b):
                pass

            @classmethod
            def bad_callback3(cls, wait_on):
                pass

            def good_callback(self, wait_on):
                pass

            @override
            def _run(self):
                pass

        p = P()
        with self.assertRaises(AssertionError):
            validate_callback_func(p.bad_callback1)
        with self.assertRaises(AssertionError):
            validate_callback_func(p.bad_callback2)
        with self.assertRaises(AssertionError):
            validate_callback_func(p.bad_callback3)

        validate_callback_func(p.good_callback)
