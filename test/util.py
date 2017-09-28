import unittest
import plum.stack as stack


class TestCase(unittest.TestCase):
    def setUp(self):
        self.assertEqual(len(stack.stack()), 0, "The stack is not empty")

    def tearDown(self):
        self.assertEqual(len(stack.stack()), 0, "The stack is not empty")
