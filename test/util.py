import apricotpy
import apricotpy.persistable
import functools
import plum
import plum.stack as stack
import unittest


class TestCase(unittest.TestCase):
    def setUp(self):
        self.assertEqual(len(stack.stack()), 0, "The stack is not empty")

    def tearDown(self):
        self.assertEqual(len(stack.stack()), 0, "The stack is not empty")


class TestCaseWithLoop(TestCase):
    def setUp(self):
        super(TestCaseWithLoop, self).setUp()
        self.loop = plum.new_event_loop()
        plum.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()
        self.loop = None


class MaxTicks(apricotpy.persistable.AwaitableMixin,
               apricotpy.persistable.PersistableLoopObjectMixin,
               apricotpy.TickingLoopObject):
    def __init__(self, max_ticks, awaitable, loop=None):
        super(MaxTicks, self).__init__(loop=loop)
        self._max_ticks = max_ticks
        awaitable.add_done_callback(self._awaitable_done)
        self._ticks = 0

    def tick(self):
        self._ticks += 1
        if not self.done() and self._ticks >= self._max_ticks:
            self.cancel()
            self.pause()

    def _awaitable_done(self, awaitable):
        if awaitable.cancelled():
            self.cancel()
        elif awaitable.exception() is not None:
            self.set_exception(awaitable.exception())
        else:
            self.set_result(awaitable.result())
        self.pause()


def get_message(receive_list, loop, subject, to, body, sender_id):
    receive_list.append({
        'subject': subject,
        'to': to,
        'body': body,
        'sender_id': sender_id
    })


def get_message_capture_fn(capture_list):
    return functools.partial(get_message, capture_list)
