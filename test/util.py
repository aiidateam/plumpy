import apricotpy
import unittest
import plum
import plum.stack as stack


class TestCase(unittest.TestCase):
    def setUp(self):
        self.assertEqual(len(stack.stack()), 0, "The stack is not empty")

    def tearDown(self):
        self.assertEqual(len(stack.stack()), 0, "The stack is not empty")


class TestCaseWithLoop(TestCase):
    def setUp(self):
        super(TestCaseWithLoop, self).setUp()
        self.loop = plum.loop_factory()
        apricotpy.set_event_loop(self.loop)
