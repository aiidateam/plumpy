"""Utilities for tests"""

from __future__ import absolute_import
import unittest

from tornado import testing

import plumpy


class TestCase(unittest.TestCase):
    pass


class TestCaseWithLoop(unittest.TestCase):
    """Test case with an event loop"""

    def setUp(self):
        super(TestCaseWithLoop, self).setUp()
        self.loop = plumpy.new_event_loop()
        plumpy.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()
        self.loop = None
        plumpy.set_event_loop(None)


def run_loop_with_timeout(loop, timeout=2.):
    loop.call_later(timeout, loop.stop)
    loop.start()


class AsyncTestCase(testing.AsyncTestCase):
    """Out custom version of the async test case from tornado"""

    def setUp(self):
        super(AsyncTestCase, self).setUp()
        self.loop = self.io_loop
