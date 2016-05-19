
import unittest
from plum.process import Process, ProcessListener


class DummyProcess(Process):
    def _run(self, **kwargs):
        self._out("default", 5)


class ProcessTest(unittest.TestCase):

    class EventsTester(ProcessListener):
        def __init__(self):
            self.starting = False
            self.finalising = False
            self.finished = False
            self.emitted = False

        def on_process_starting(self, process, inputs):
            self.starting = True

        def on_process_finalising(self, process):
            self.finalising = True

        def on_process_finished(self, process, retval):
            self.finished = True

        def on_output_emitted(self, process, output_port, value, dynamic):
            self.emitted = True

    def events_test(self):
        events_tester = self.EventsTester()

        proc = DummyProcess()
        proc.add_process_listener(events_tester)

        proc.on_start({}, None)
        self.assertTrue(events_tester.starting)

        proc._run()
        self.assertTrue(events_tester.emitted)

        proc.on_finalise()
        self.assertTrue(events_tester.finalising)

        proc.on_finish(None)
        self.assertTrue(events_tester.finished)

        proc.remove_process_listener(events_tester)
