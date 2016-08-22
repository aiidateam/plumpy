from unittest import TestCase

from plum.test_utils import ProcessListenerTester
from plum.engine.serial import SerialEngine
from plum.process import Process, ProcessState
from plum.util import override
from plum.process_monitor import MONITOR


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
    def on_create(self, pid, inputs, saved_instance_state):
        pass

    @override
    def on_run(self):
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
    def on_finish(self):
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
        MONITOR.reset()

    def test_on_run(self):
        self.proc.on_run()
        self.assertTrue(self.events_tester.run)

    def test_on_output_emitted(self):
        self.proc._run()
        self.assertTrue(self.events_tester.emitted)

    def test_on_destroy(self):
        self.proc.on_destroy()
        self.assertTrue(self.events_tester.destroy)

    def test_on_finished(self):
        self.proc.on_finish()
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
        p.perform_create(0, {'a': 5})
        self.assertEqual(p.inputs.a, 5)
        with self.assertRaises(AttributeError):
            p.inputs.b

    def test_run(self):
        engine = SerialEngine()
        results = DummyProcess.run(None, engine)
        self.assertEqual(results['default'], 5)

    def test_forget_to_call_parent(self):
        p = ForgetToCallParent()

        with self.assertRaises(AssertionError):
            p.perform_create(None, None)

        with self.assertRaises(AssertionError):
            p.perform_run(None)

        with self.assertRaises(AssertionError):
            p.perform_wait(None)

        with self.assertRaises(AssertionError):
            p.perform_continue(None)

        with self.assertRaises(AssertionError):
            p.perform_finish()

        with self.assertRaises(AssertionError):
            p.perform_stop()

        with self.assertRaises(AssertionError):
            p.perform_destroy()

    def test_pid(self):
        # Test auto generation of pid
        p = DummyProcess.new_instance()
        self.assertIsNotNone(p.pid)

        # Test using integer as pid
        p = DummyProcess.new_instance(pid=5)
        self.assertEquals(p.pid, 5)

        # Test using string as pid
        p = DummyProcess.new_instance(pid='a')
        self.assertEquals(p.pid, 'a')

    def test_tick(self):
        proc = DummyProcess.new_instance()
        self.assertEqual(proc.state, ProcessState.CREATED)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.RUNNING)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.FINISHED)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.STOPPED)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.DESTROYED)

    def test_instance_state(self):
        pass
