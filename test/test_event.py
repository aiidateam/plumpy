
from util import TestCase
from plum.event import ProcessMonitorEmitter, WaitOnProcessEvent, EmitterAggregator, EventEmitter
from plum.test_utils import DummyProcess, ExceptionProcess
from plum.util import ListenContext


class _EventSaver(object):
    def __init__(self, emitter=None):
        self.events = []
        if emitter is not None:
            emitter.start_listening(self.event_ocurred)

    def event_ocurred(self, emitter, evt, body):
        self.events.append(evt)


class TestProcessMonitorEmitter(TestCase):
    def setUp(self):
        super(TestProcessMonitorEmitter, self).setUp()
        self.emitter = ProcessMonitorEmitter()

    def test_normal_process(self):
        saver = _EventSaver()
        self.emitter.start_listening(saver.event_ocurred, "process.*")

        p = DummyProcess.new()
        preamble = "process.{}.".format(p.pid)
        p.play()

        self.assertEqual(saver.events, [preamble + 'finished', preamble + 'stopped'])

    def test_fail_process(self):
        saver = _EventSaver()
        self.emitter.start_listening(saver.event_ocurred, "process.*")

        p = ExceptionProcess.new()
        preamble = "process.{}".format(p.pid)
        p.play()
        self.assertIsInstance(p.get_exception(), RuntimeError)
        self.assertEqual(saver.events, [preamble + '.failed'])


class TestWaitOnProcessEvent(TestCase):
    def setUp(self):
        super(TestWaitOnProcessEvent, self).setUp()
        self.emitter = ProcessMonitorEmitter()

    def test_finished(self):
        p = DummyProcess.new()
        w = WaitOnProcessEvent(self.emitter, p.pid, "finished")
        p.play()
        self.assertTrue(w.is_done())

    def test_stopped(self):
        p = DummyProcess.new()
        w = WaitOnProcessEvent(self.emitter, p.pid, "stopped")
        p.play()
        self.assertTrue(w.is_done())

    def test_failed(self):
        p = ExceptionProcess.new()
        w = WaitOnProcessEvent(self.emitter, p.pid, "failed")
        p.play()
        self.assertIsInstance(p.get_exception(), RuntimeError)
        self.assertTrue(w.is_done())


class _EmitterTester(EventEmitter):
    """
    A simple emitter that just passes on messages
    """
    def emit(self, event):
        self.event_occurred(event)


class TestEventEmitter(TestCase):
    def setUp(self):
        super(TestEventEmitter, self).setUp()
        self.last = None
        self.emitter = _EmitterTester()

    def test_listen_all(self):
        # This should listen for all events
        self.emitter.start_listening(self._receive, '*')

        # Simple message
        self.emitter.emit("Hello")
        self.assertEqual(self.last[1], "Hello")

        # More complex
        self.emitter.emit("Hello.how.are.you?")
        self.assertEqual(self.last[1], "Hello.how.are.you?")

    def test_stop_listening_specific(self):
        self.emitter.start_listening(self._receive, "hello")
        self.assertEqual(self.emitter.num_listening(), 1)
        self.emitter.stop_listening(self._receive, "hello")
        self.assertEqual(self.emitter.num_listening(), 0)

        # Not listening
        self.emitter.emit("hello")
        self.assertIsNone(self.last)

    def test_stop_listening_all(self):
        self.emitter.start_listening(self._receive)
        self.emitter.start_listening(self._receive, "hello")
        self.assertEqual(self.emitter.num_listening(), 2)
        self.emitter.stop_listening(self._receive)
        self.assertEqual(self.emitter.num_listening(), 0)

        # Not listening
        self.emitter.emit("hello")
        self.assertIsNone(self.last)

    def test_listen_specific(self):
        self.emitter.start_listening(self._receive, "hello")

        # Check we get the one we want to hear
        self.emitter.emit("hello")
        self.assertEqual(self.last[1], "hello")

        # Check we don't get what we don't won't to hear
        self.last = None
        self.emitter.emit("goodbye")
        self.assertIsNone(self.last)

    def _receive(self, emitter, evt, body):
        self.last = emitter, evt, body

    def test_listen_wildcard(self):
        self.emitter.start_listening(self._receive, "martin.*")

        # Check I get my message
        self.emitter.emit("martin.hello")
        self.assertEqual(self.last[1], "martin.hello")

        # Check I don't get messages for someone else
        self.last = None
        self.emitter.emit("giovanni.hello")
        self.assertIsNone(self.last)

    def test_listen_wildcard_hash(self):
        self.emitter.start_listening(self._receive, "##")
        self.emitter.emit("12")
        self.assertEqual(self.last[1], "12")

        self.last = None
        self.emitter.emit("1")
        self.assertIsNone(self.last)


class TestEmitterAggregator(TestCase):
    def setUp(self):
        super(TestEmitterAggregator, self).setUp()
        self.last = None
        self.aggregator = EmitterAggregator()

    def test_listen(self):
        e1 = _EmitterTester()
        e2 = _EmitterTester()

        self.aggregator.add_child(e1)
        self.aggregator.add_child(e2)

        self.aggregator.start_listening(self._receive)

        e1.emit("e1")
        self.assertEqual(self.last[1], "e1")

        e2.emit("e2")
        self.assertEqual(self.last[1], "e2")

    def test_stop_listening_specific(self):
        e1 = _EmitterTester()
        e2 = _EmitterTester()
        self.aggregator.add_child(e1)
        self.aggregator.add_child(e2)

        # Start listening
        self.aggregator.start_listening(self._receive, "hello")
        self.assertEqual(self.aggregator.num_listening(), 1)
        self.assertEqual(e1.num_listening(), 1)
        self.assertEqual(e2.num_listening(), 1)

        # Stop listening
        self.aggregator.stop_listening(self._receive, "hello")
        self.assertEqual(self.aggregator.num_listening(), 0)
        self.assertEqual(e1.num_listening(), 0)
        self.assertEqual(e2.num_listening(), 0)

    def test_stop_listening_wildcard(self):
        e1 = _EmitterTester()
        e2 = _EmitterTester()
        self.aggregator.add_child(e1)
        self.aggregator.add_child(e2)

        # Start listening
        self.aggregator.start_listening(self._receive)
        self.assertEqual(self.aggregator.num_listening(), 1)
        self.assertEqual(e1.num_listening(), 1)
        self.assertEqual(e2.num_listening(), 1)

        # Stop listening
        self.aggregator.stop_listening(self._receive)
        self.assertEqual(self.aggregator.num_listening(), 0)
        self.assertEqual(e1.num_listening(), 0)
        self.assertEqual(e2.num_listening(), 0)

    def _receive(self, emitter, evt, body):
        self.last = emitter, evt, body





