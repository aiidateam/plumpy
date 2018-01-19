import functools
import plum
import plum.stack as stack
import unittest


class TestCase(unittest.TestCase):
    def setUp(self):
        self.assertTrue(stack.is_empty(), "The stack is not empty")

    def tearDown(self):
        self.assertTrue(stack.is_empty(), "The stack is not empty")


class TestCaseWithLoop(TestCase):
    def setUp(self):
        super(TestCaseWithLoop, self).setUp()
        self.loop = plum.new_event_loop()
        plum.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()
        self.loop = None
        plum.set_event_loop(None)


def get_message(receive_list, loop, subject, to, body, sender_id):
    receive_list.append({
        'subject': subject,
        'to': to,
        'body': body,
        'sender_id': sender_id
    })


def get_message_capture_fn(capture_list):
    return functools.partial(get_message, capture_list)


def run_loop_with_timeout(loop, timeout=2.):
    loop.call_later(timeout, loop.stop)
    loop.start()