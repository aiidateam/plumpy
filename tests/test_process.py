from unittest import TestCase

from plum.test_utils import ProcessListenerTester
from plum.engine.serial import SerialEngine
from plum.process import Process
from plum.util import override



class DummyProcess(Process):
    @classmethod
    def _define(cls, spec):
        spec.dynamic_input()
        spec.dynamic_output()

    def _run(self, **kwargs):
        self.out("default", 5)


class ForgetToCallParent(Process):
    @override
    def _run(self):
        pass

    @override
    def on_create(self, pid, inputs=None):
        pass

    @override
    def on_recreate(self, pid, saved_instance_state):
        pass

    @override
    def on_start(self):
        pass

    @override
    def on_wait(self, wait_on):
        pass

    @override
    def on_continue(self, wait_on):
        pass

    @override
    def on_fail(self, exception):
        pass

    @override
    def on_finish(self, retval):
        pass

    @override
    def on_stop(self):
        pass

    @override
    def on_destroy(self):
        pass


class TestProcess(TestCase):
    def setUp(self):
        self.events_tester = ProcessListenerTester()
        self.proc = DummyProcess()
        self.proc.add_process_listener(self.events_tester)

    def tearDown(self):
        self.proc.remove_process_listener(self.events_tester)
        if not self.proc._called_on_destroy:
            self.proc.on_destroy()

    def test_on_start(self):
        self.proc.on_start()
        self.assertTrue(self.events_tester.start)

    def test_on_output_emitted(self):
        self.proc._run()
        self.assertTrue(self.events_tester.emitted)

    def test_on_destroy(self):
        self.proc.on_destroy()
        self.assertTrue(self.events_tester.destroy)

    def test_on_finished(self):
        self.proc.on_finish(None)
        self.assertTrue(self.events_tester.finish)

    def test_dynamic_inputs(self):
        class NoDynamic(Process):
            def _run(self, **kwargs):
                pass

        class WithDynamic(Process):
            @classmethod
            def _define(cls, spec):
                spec.dynamic_input()

            def _run(self, **kwargs):
                pass

        with self.assertRaises(ValueError):
            NoDynamic.run(inputs={'a': 5})
        WithDynamic.run(inputs={'a': 5})

    def test_inputs(self):
        class Proc(Process):
            @classmethod
            def _define(cls, spec):
                spec.input('a')

            def _run(self, a):
                pass

        p = Proc()

        # Check that we can't access inputs before creating
        with self.assertRaises(AttributeError):
            p.inputs.a

        # Check that we can access the inputs after creating
        p.on_create(0, {'a': 5})
        self.assertEqual(p.inputs.a, 5)
        with self.assertRaises(AttributeError):
            p.inputs.b
        p.on_destroy()

        # Check that we can't access inputs after finishing
        p = Proc()
        p.run(inputs={'a': 5})
        with self.assertRaises(AttributeError):
            p.inputs.a
        p.on_destroy()

    def test_run(self):
        engine = SerialEngine()
        results = DummyProcess.run(None, engine)
        self.assertEqual(results['default'], 5)

    def test_forget_to_call_parent(self):
        p = ForgetToCallParent()

        with self.assertRaises(AssertionError):
            p.signal_on_create(None, None)

        with self.assertRaises(AssertionError):
            p.signal_on_recreate(None, None)

        with self.assertRaises(AssertionError):
            p.signal_on_start(None, None)

        with self.assertRaises(AssertionError):
            p.signal_on_wait(None)

        with self.assertRaises(AssertionError):
            p.signal_on_continue(None)

        with self.assertRaises(AssertionError):
            p.signal_on_fail(None)

        with self.assertRaises(AssertionError):
            p.signal_on_finish(None)

        with self.assertRaises(AssertionError):
            p.signal_on_stop()

        with self.assertRaises(AssertionError):
            p.signal_on_destroy()
