from .util import TestCase
import plum.test_utils
import plum.stack as stack
from plum.process_manager import async


class TestStack(TestCase):
    def test_simple(self):
        p = plum.test_utils.WaitForSignalProcess.new()
        self.assertEqual(len(stack.stack()), 0)
        async(p)
        self.assertEqual(len(stack.stack()), 0)
        self.assertTrue(p.abort(timeout=2.))
        self.assertEqual(len(stack.stack()), 0)
