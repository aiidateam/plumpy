from .util import TestCase
from plum.event import wait_on_process_event, EventEmitter
from plum import loop_factory
from plum.test_utils import DummyProcess, ExceptionProcess


class _EventSaver(object):
    def __init__(self, emitter=None):
        self.events = []
        if emitter is not None:
            emitter.add_listener(self.event_ocurred)

    def event_ocurred(self, emitter, evt, body):
        self.events.append(evt)


class TestWaitOnProcessEvent(TestCase):
    def setUp(self):
        super(TestWaitOnProcessEvent, self).setUp()
        self.loop = loop_factory()

    def tearDown(self):
        self.loop.close()
        self.loop = None

    def test_finished_stopped(self):
        for event in ("finish", "stop"):
            p = self.loop.create(DummyProcess)

            wait_on = wait_on_process_event(self.loop, p.pid, event)
            self.loop.run_until_complete(wait_on)

    def test_failed(self):
        p = self.loop.create(ExceptionProcess)
        wait_on = wait_on_process_event(self.loop, p.pid, 'fail')
        self.loop.run_until_complete(wait_on)


class _EmitterTester(EventEmitter):
    """
    A simple emitter that just passes on messages
    """

    def emit(self, event):
        self.event_occurred(event)


class TestEventEmitter(TestCase):
    def setUp(self):
        super(TestEventEmitter, self).setUp()
        self.loop = loop_factory()
        self.last = None

    def test_listen_all(self):
        # This should listen for all events
        self.loop.messages().add_listener(self._receive, '*')

        # Simple message
        self.loop.messages().send("Hello")
        self.loop.tick()
        self.assertEqual(self.last[1], "Hello")

        # More complex
        self.loop.messages().send("Hello.how.are.you?")
        self.loop.tick()
        self.assertEqual(self.last[1], "Hello.how.are.you?")

    def test_stop_listening_specific(self):
        self.loop.messages().add_listener(self._receive, "hello")
        self.assertEqual(self.loop.messages().num_listening(), 1)
        self.loop.messages().remove_listener(self._receive, "hello")
        self.assertEqual(self.loop.messages().num_listening(), 0)

        # Not listening
        self.loop.messages().send("hello")
        self.assertIsNone(self.last)

    def test_stop_listening_all(self):
        self.loop.messages().add_listener(self._receive)
        self.loop.messages().add_listener(self._receive, "hello")
        self.assertEqual(self.loop.messages().num_listening(), 2)
        self.loop.messages().remove_listener(self._receive)
        self.assertEqual(self.loop.messages().num_listening(), 0)

        # Not listening
        self.loop.messages().send("hello")
        self.assertIsNone(self.last)

    def test_listen_specific(self):
        self.loop.messages().add_listener(self._receive, "hello")

        # Check we get the one we want to hear
        self.loop.messages().send("hello")
        self.loop.tick()
        self.assertEqual(self.last[1], "hello")

        # Check we don't get what we don't won't to hear
        self.last = None
        self.loop.messages().send("goodbye")
        self.loop.tick()
        self.assertIsNone(self.last)

    def test_listen_wildcard(self):
        self.loop.messages().add_listener(self._receive, "martin.*")
        self.loop.tick()

        # Check I get my message
        self.loop.messages().send("martin.hello")
        self.loop.tick()
        self.assertEqual(self.last[1], "martin.hello")

        # Check I don't get messages for someone else
        self.last = None
        self.loop.messages().send("giovanni.hello")
        self.loop.tick()
        self.assertIsNone(self.last)

    def test_listen_wildcard_hash(self):
        self.loop.messages().add_listener(self._receive, "##")
        self.loop.messages().send("12")
        self.loop.tick()
        self.assertEqual(self.last[1], "12")

        self.last = None
        self.loop.messages().send("1")
        self.loop.tick()
        self.assertIsNone(self.last)

    def _receive(self, emitter, evt, body, sender_id):
        self.last = emitter, evt, body, sender_id
