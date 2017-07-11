import unittest
import plum.stack as stack
from plum.process_monitor import MONITOR


class TestCase(unittest.TestCase):
    def setUp(self):
        self.assertEqual(len(MONITOR.get_pids()), 0,
                         "One or more processes are still running")
        self.assertEqual(len(stack.stack()), 0, "The stack is not empty")

    def tearDown(self):
        self.assertEqual(len(MONITOR.get_pids()), 0,
                         "One or more processes are still running")
        self.assertEqual(len(stack.stack()), 0, "The stack is not empty")

    def safe_join(self, thread, timeout=1):
        thread.join(timeout)
        self.assertFalse(thread.is_alive())
