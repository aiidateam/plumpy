
import unittest
import threading
from plum.process_monitor import MONITOR


class TestCase(unittest.TestCase):
    def setUp(self):
        self.assertEqual(
            len(MONITOR.get_pids()), 0,
            "One or more processes are still running")
        self.__threads_at_start = threading.active_count()

    def tearDown(self):
        self.assertEqual(
            len(MONITOR.get_pids()), 0,
            "One or more processes are still running")
        self.assertEqual(threading.active_count(), self.__threads_at_start)

    def safe_join(self, thread, timeout=1):
        thread.join(timeout)
        self.assertFalse(thread.is_alive())