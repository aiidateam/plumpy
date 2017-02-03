from plum.event import ProcessMonitorEmitter, WaitOnProcessEvent
from util import TestCase
from plum.test_utils import DummyProcess, ExceptionProcess


class _EventSaver(object):
    def __init__(self, emitter=None):
        self.events = []
        if emitter is not None:
            emitter.start_listening(self.event_ocurred)

    def event_ocurred(self, emitter, event):
        self.events.append(event)


class TestProcessMonitorEmitter(TestCase):
    def setUp(self):
        super(TestProcessMonitorEmitter, self).setUp()
        self.emitter = ProcessMonitorEmitter()

    def test_normal_process(self):
        saver = _EventSaver()
        self.emitter.start_listening(saver.event_ocurred, "process.*")

        p = DummyProcess.new_instance()
        preamble = "process.{}.".format(p.pid)
        p.play()

        self.assertEqual(saver.events, [preamble + 'finished', preamble + 'stopped'])

    def test_fail_process(self):
        saver = _EventSaver()
        self.emitter.start_listening(saver.event_ocurred, "process.*")

        p = ExceptionProcess.new_instance()
        preamble = "process.{}".format(p.pid)

        with self.assertRaises(RuntimeError):
            p.play()

        self.assertEqual(saver.events, [preamble + '.failed'])


class TestWaitOnProcessEvent(TestCase):
    def setUp(self):
        super(TestWaitOnProcessEvent, self).setUp()
        self.emitter = ProcessMonitorEmitter()

    def test_finished(self):
        p = DummyProcess.new_instance()
        w = WaitOnProcessEvent(self.emitter, p.pid, "finished")
        p.play()
        self.assertTrue(w.is_done())

    def test_stopped(self):
        p = DummyProcess.new_instance()
        w = WaitOnProcessEvent(self.emitter, p.pid, "stopped")
        p.play()
        self.assertTrue(w.is_done())

    def test_failed(self):
        p = ExceptionProcess.new_instance()
        w = WaitOnProcessEvent(self.emitter, p.pid, "failed")
        with self.assertRaises(RuntimeError):
            p.play()
        self.assertTrue(w.is_done())
