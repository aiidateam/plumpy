"""Utilities for tests"""

import shortuuid
import unittest

import kiwipy.rmq
from tornado import testing, gen
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


class AsyncTestCase(testing.AsyncTestCase):
    """Our custom version of the async test case from tornado"""

    communicator = None

    def setUp(self):
        super(AsyncTestCase, self).setUp()
        self.loop = self.io_loop

    def init_communicator(self):
        """
        Create a testing communicator and set it to self.communicator

        :return: the created communicator
        :rtype: :class:`kiwipy.Communicator`
        """
        message_exchange = "{}.{}".format(self.__class__.__name__, shortuuid.uuid())
        task_exchange = "{}.{}".format(self.__class__.__name__, shortuuid.uuid())
        task_queue = "{}.{}".format(self.__class__.__name__, shortuuid.uuid())

        self.communicator = kiwipy.rmq.connect(
            connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
            message_exchange=message_exchange,
            task_exchange=task_exchange,
            task_queue=task_queue,
            testing_mode=True)

        return self.communicator


@gen.coroutine
def wait_util(condition, sleep_interval=0.1):
    """Given a condition function, keep polling until it returns True"""
    while not condition():
        yield gen.sleep(sleep_interval)
