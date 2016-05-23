from unittest import TestCase
from plum.process import Process, ProcessListener


class DummyProcess(Process):
    @staticmethod
    def _define(spec):
        spec.dynamic_output()

    def _run(self, **kwargs):
        self._out("default", 5)


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


class TestProcess(TestCase):
    def test_events(self):
        events_tester = EventsTester()

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

    def test_dynamic_inputs(self):
        class NoDynamic(Process):
            @staticmethod
            def _define(spec):
                pass

            def _run(self, **kwargs):
                pass

        class WithDynamic(Process):
            @staticmethod
            def _define(spec):
                spec.dynamic_input()

            def _run(self, **kwargs):
                pass

        with self.assertRaises(RuntimeError):
            NoDynamic.create().run(inputs={'a': 5})
        WithDynamic.create().run(inputs={'a': 5})

    def test_attributes(self):
        class Proc(Process):
            @staticmethod
            def _define(spec):
                spec.attribute("a", required=True, valid_type=int)
                spec.attribute("b", required=False, default='hello')

            def _run(self, **kwargs):
                pass

        with self.assertRaises(ValueError):
            p = Proc()

        p = Proc(attributes={'a': 5})
        self.assertEqual(p._attributes['b'], 'hello')

        with self.assertRaises(ValueError):
            p = Proc(attributes={'b': 2})

        with self.assertRaises(ValueError):
            p = Proc(attributes={'a': 'hello'})



