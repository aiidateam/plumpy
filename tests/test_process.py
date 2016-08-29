from unittest import TestCase

from plum.test_utils import ProcessListenerTester
from plum.process import Process, ProcessState
from plum.util import override
from plum.test_utils import ExceptionProcess, TwoCheckpointProcess,\
    DummyProcessWithOutput, TEST_PROCESSES
from plum.persistence.bundle import Bundle
from plum.process_monitor import MONITOR


class ForgetToCallParent(Process):
    @override
    def _run(self):
        pass

    @override
    def on_create(self, pid, inputs, saved_instance_state):
        pass

    @override
    def on_start(self):
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
        self.proc = DummyProcessWithOutput()
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
                super(WithDynamic, cls)._define(spec)

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
                super(Proc, cls)._define(spec)

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
        dp = DummyProcessWithOutput.new_instance()
        dp.run_until_complete()

        self.assertTrue(dp.has_finished())
        self.assertEqual(dp.state, ProcessState.DESTROYED)
        self.assertEqual(dp.outputs, {'default': 5})

    def test_run_from_class(self):
        # Test running through class method
        results = DummyProcessWithOutput.run()
        self.assertEqual(results['default'], 5)

    def test_forget_to_call_parent(self):
        p = ForgetToCallParent()

        with self.assertRaises(AssertionError):
            p.perform_create(None, None, None)

        with self.assertRaises(AssertionError):
            p.perform_start()

        with self.assertRaises(AssertionError):
            p.perform_run()

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
        p = DummyProcessWithOutput.new_instance()
        self.assertIsNotNone(p.pid)

        # Test using integer as pid
        p = DummyProcessWithOutput.new_instance(pid=5)
        self.assertEquals(p.pid, 5)

        # Test using string as pid
        p = DummyProcessWithOutput.new_instance(pid='a')
        self.assertEquals(p.pid, 'a')

    def test_tick_simple(self):
        proc = DummyProcessWithOutput.new_instance()
        self.assertEqual(proc.state, ProcessState.CREATED)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.STARTED)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.FINISHED)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.STOPPED)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.DESTROYED)
        del proc

    def test_tick_exception(self):
        proc = ExceptionProcess.new_instance()
        self.assertEqual(proc.state, ProcessState.CREATED)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.STARTED)
        with self.assertRaises(RuntimeError):
            proc.tick()
        self.assertEqual(proc.state, ProcessState.RUNNING)
        del proc

    def test_tick_two_checkpoints(self):
        proc = TwoCheckpointProcess.new_instance()
        self.assertEqual(proc.state, ProcessState.CREATED)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.STARTED)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.WAITING)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.WAITING)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.FINISHED)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.STOPPED)
        proc.tick()
        self.assertEqual(proc.state, ProcessState.DESTROYED)
        del proc

    def test_instance_state(self):
        proc = TwoCheckpointProcess.new_instance()
        proc.run_until(ProcessState.WAITING)
        b = Bundle()
        proc.save_instance_state(b)
        self.assertEqual(proc.outputs, b[Process.BundleKeys.OUTPUTS.value].get_dict())

        proc.stop()
        proc.run_until_complete()

        proc = TwoCheckpointProcess.create_from(b)
        proc.run_until(ProcessState.WAITING)
        b = Bundle()
        proc.save_instance_state(b)
        self.assertEqual(proc.outputs, b[Process.BundleKeys.OUTPUTS.value].get_dict())

        proc.stop()
        proc.run_until_complete()

    def test_saving_each_step(self):
        from collections import namedtuple

        ProcInfo = namedtuple('ProcInfo', ['state', 'bundle', 'outputs'])

        for ProcClass in TEST_PROCESSES:
            proc = ProcClass.new_instance()
            snapshots = list()
            while proc.state is not ProcessState.DESTROYED:
                b = Bundle()
                proc.save_instance_state(b)
                snapshots.append(ProcInfo(proc.state, b, proc.outputs.copy()))
                # The process may crash, so catch it here
                try:
                    proc.tick()
                except BaseException:
                    break

            for i, info in zip(range(0, len(snapshots)), snapshots):
                loaded = ProcClass.create_from(info.bundle)
                # Get the process back to the state it was in when it was saved
                loaded.run_until(info.state)

                # Now go forward from that point in making sure the bundles match
                j = i
                while loaded.state is not ProcessState.DESTROYED:
                    snapshot = snapshots[j]
                    self.assertEqual(snapshot.state, loaded.state)

                    new_bundle = Bundle()
                    loaded.save_instance_state(new_bundle)
                    self.assertEqual(snapshot.bundle, new_bundle,
                                     "Bundle mismatch with process class {}\n"
                                     "Original after {} ticks:\n{}\n"
                                     "Loaded after {} ticks:\n{}".format(
                                         ProcClass, j, snapshot.bundle, j - i, new_bundle))

                    self.assertEqual(snapshot.outputs, loaded.outputs,
                                     "Outputs mismatch with process class {}\n"
                                     "Original after {} ticks:\n{}\n"
                                     "Loaded after {} ticks:\n{}".format(
                                         ProcClass, j, snapshot.outputs, j - i, loaded.outputs))
                    try:
                        loaded.tick()
                    except BaseException:
                        break

                    j += 1

