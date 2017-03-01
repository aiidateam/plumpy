
import unittest
import threading
from plum.process_monitor import MONITOR


class TestCase(unittest.TestCase):
    def setUp(self):
        self.assertEqual(
            len(MONITOR.get_pids()), 0,
            "One or more processes are still running")

    def tearDown(self):
        self.assertEqual(
            len(MONITOR.get_pids()), 0,
            "One or more processes are still running")

    def safe_join(self, thread, timeout=1):
        thread.join(timeout)
        self.assertFalse(thread.is_alive())
