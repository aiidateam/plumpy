import apricotpy
import plum
from plum import loop_factory
from plum import Process, ProcessState
from plum.test_utils import DummyProcess, ExceptionProcess, DummyProcessWithOutput, TEST_PROCESSES, ProcessSaver, \
    check_process_against_snapshots, \
    WaitForSignalProcess
from plum.test_utils import ProcessListenerTester
from plum.utils import override
from plum.wait_ons import run_until, WaitOnProcessState
from .util import TestCase


class ForgetToCallParent(Process):
    @classmethod
    def define(cls, spec):
        super(ForgetToCallParent, cls).define(spec)
        spec.input('forget_on', valid_type=str)

    @override
    def _run(self, forget_on):
        pass

    @override
    def on_create(self):
        if self.inputs.forget_on != 'create':
            super(ForgetToCallParent, self).on_create()

    @override
    def on_start(self):
        if self.inputs.forget_on != 'start':
            super(ForgetToCallParent, self).on_start()

    @override
    def on_run(self):
        if self.inputs.forget_on != 'run':
            super(ForgetToCallParent, self).on_start()

    @override
    def on_fail(self, exc_info):
        if self.inputs.forget_on != 'fail':
            super(ForgetToCallParent, self).on_start()

    @override
    def on_finish(self):
        if self.inputs.forget_on != 'finish':
            super(ForgetToCallParent, self).on_start()

    @override
    def on_stop(self):
        if self.inputs.forget_on != 'stop':
            super(ForgetToCallParent, self).on_start()


class TestProcess(TestCase):
    def setUp(self):
        super(TestProcess, self).setUp()
        self.loop = loop_factory()

    def test_spec(self):
        """
        Check that the references to specs are doing the right thing...
        """
        dp = self.loop.create(DummyProcess)
        self.assertIsNot(DummyProcess.spec(), Process.spec())
        self.assertIs(dp.spec(), DummyProcess.spec())

        class Proc(DummyProcess):
            pass

        self.assertIsNot(Proc.spec(), Process.spec())
        self.assertIsNot(Proc.spec(), DummyProcess.spec())
        p = self.loop.create(Proc)
        self.assertIs(p.spec(), Proc.spec())

    def test_dynamic_inputs(self):
        class NoDynamic(Process):
            def _run(self, **kwargs):
                pass

        class WithDynamic(Process):
            @classmethod
            def define(cls, spec):
                super(WithDynamic, cls).define(spec)

                spec.dynamic_input()

            def _run(self, **kwargs):
                pass

        with self.assertRaises(ValueError):
            self.loop.run_until_complete(self.loop.create(NoDynamic, {'a': 5}))

        self.loop.run_until_complete(self.loop.create(WithDynamic, {'a': 5}))

    def test_inputs(self):
        class Proc(Process):
            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)
                spec.input('a')

            def _run(self, a):
                pass

        p = self.loop.create(Proc, {'a': 5})

        # Check that we can access the inputs after creating
        self.assertEqual(p.raw_inputs.a, 5)
        with self.assertRaises(AttributeError):
            p.raw_inputs.b

    def test_inputs_default(self):
        class Proc(DummyProcess):
            @classmethod
            def define(cls, spec):
                super(Proc, cls).define(spec)
                spec.input('input', default=5, required=False)

        # Supply a value
        p = self.loop.create(Proc, inputs={'input': 2})
        self.assertEqual(p.inputs['input'], 2)

        # Don't supply, use default
        p = self.loop.create(Proc)
        self.assertEqual(p.inputs['input'], 5)

    def test_inputs_default_that_evaluate_to_false(self):
        for def_val in (True, False, 0, 1):
            class Proc(DummyProcess):
                @classmethod
                def define(cls, spec):
                    super(Proc, cls).define(spec)
                    spec.input('input', default=def_val)

            # Don't supply, use default
            p = self.loop.create(Proc)
            self.assertIn('input', p.inputs)
            self.assertEqual(p.inputs['input'], def_val)

    def test_run(self):
        p = self.loop.create(DummyProcessWithOutput)
        self.loop.run_until_complete(p)

        self.assertTrue(p.has_finished())
        self.assertEqual(p.state, ProcessState.STOPPED)
        self.assertEqual(p.outputs, {'default': 5})

    def test_run_from_class(self):
        # Test running through class method

        results = self.loop.run_until_complete(
            self.loop.create(DummyProcessWithOutput)
        )
        self.assertEqual(results['default'], 5)

    def test_forget_to_call_parent(self):
        for event in ('create', 'start', 'run', 'finish', 'stop'):
            with self.assertRaises(AssertionError):
                self.loop.run_until_complete(
                    self.loop.create(ForgetToCallParent, {'forget_on': event})
                )

    def test_pid(self):
        # Test auto generation of pid
        p = self.loop.create(DummyProcessWithOutput)
        self.assertIsNotNone(p.pid)

        # Test using integer as pid
        p = self.loop.create(DummyProcessWithOutput, pid=5)
        self.assertEquals(p.pid, 5)

        # Test using string as pid
        p = self.loop.create(DummyProcessWithOutput, pid='a')
        self.assertEquals(p.pid, 'a')

    def test_exception(self):
        proc = self.loop.create(ExceptionProcess)
        with self.assertRaises(RuntimeError):
            self.loop.run_until_complete(proc)
        self.assertEqual(proc.state, ProcessState.FAILED)

    def test_get_description(self):
        # Not all that much we can test for, but check if it's a string at
        # least
        for ProcClass in TEST_PROCESSES:
            desc = ProcClass.get_description()
            self.assertIsInstance(desc, str)

        # Dummy process should at least use the docstring as part of the
        # description and so it shouldn't be empty
        desc = DummyProcess.get_description()
        self.assertNotEqual(desc, "")

    def test_created_bundle(self):
        """
        Check that the bundle after just creating a process is as we expect
        :return:
        """
        proc = ~self.loop.create_inserted(DummyProcessWithOutput)
        b = apricotpy.persistable.Bundle(proc)

        self.assertIsNone(b.get(plum.process.BundleKeys.INPUTS, None))
        self.assertEqual(len(b[plum.process.BundleKeys.OUTPUTS]), 0)

    def test_instance_state(self):
        BundleKeys = plum.process.BundleKeys

        proc = self.loop.create(DummyProcessWithOutput)

        saver = ProcessSaver(proc)
        self.loop.run_until_complete(proc)

        for info, outputs in zip(saver.snapshots, saver.outputs):
            state, bundle = info
            # Check that it is a copy
            self.assertIsNot(outputs, bundle[BundleKeys.OUTPUTS])
            # Check the contents are the same
            self.assertEqual(outputs, bundle[BundleKeys.OUTPUTS])

        self.assertIsNot(
            proc.outputs, saver.snapshots[-1][1][BundleKeys.OUTPUTS])

    def test_saving_each_step(self):
        for ProcClass in TEST_PROCESSES:
            proc = self.loop.create(ProcClass)

            saver = ProcessSaver(proc)
            self.loop.run_until_complete(proc)

            self.assertEqual(proc.state, ProcessState.STOPPED)
            self.assertTrue(check_process_against_snapshots(self.loop, ProcClass, saver.snapshots))

    def test_saving_each_step_interleaved(self):
        for ProcClass in TEST_PROCESSES:
            proc = self.loop.create(ProcClass)
            ps = ProcessSaver(proc)
            try:
                self.loop.run_until_complete(proc)
            except BaseException:
                pass

            self.assertTrue(check_process_against_snapshots(self.loop, ProcClass, ps.snapshots))

    def test_logging(self):
        class LoggerTester(Process):
            def _run(self, **kwargs):
                self.logger.info("Test")

        # TODO: Test giving a custom logger to see if it gets used
        self.loop.run_until_complete(self.loop.create(LoggerTester))

    def test_abort(self):
        proc = ~self.loop.create_inserted(DummyProcess)

        aborted = ~proc.abort('Farewell!')
        self.assertTrue(aborted)
        self.assertTrue(proc.has_aborted())
        self.assertEqual(proc.get_abort_msg(), 'Farewell!')
        self.assertEqual(proc.state, ProcessState.STOPPED)

    def test_wait_continue(self):
        proc = self.loop.create(WaitForSignalProcess)

        # Wait - Run the process and wait until it is waiting
        run_until(proc, ProcessState.WAITING, self.loop)

        proc.continue_()

        # Run
        self.loop.run_until_complete(proc)

        # Check it's done
        self.assertEqual(proc.state, ProcessState.STOPPED)
        self.assertTrue(proc.has_finished())

    def test_exc_info(self):
        p = self.loop.create(ExceptionProcess)
        try:
            self.loop.run_until_complete(p)
        except RuntimeError as e:
            self.assertEqual(p.exception(), e)

    def test_restart(self):
        process = self.loop.create(_RestartProcess)
        run_until(process, ProcessState.WAITING, self.loop)

        # Save the state of the process
        saved_state = apricotpy.persistable.Bundle(process)
        ~self.loop.remove(process)

        # Load a process from the saved state
        process = saved_state.unbundle(self.loop)
        self.assertEqual(process.state, ProcessState.WAITING)

        # Now play it
        process.continue_()
        result = ~process
        self.assertEqual(result, {'finished': True})

    def test_run_terminated(self):
        p = self.loop.create(DummyProcess)
        self.loop.run_until_complete(p)
        self.assertTrue(p.has_terminated())

    def test_wait_pause_continue_play(self):
        """
        Test that if you pause a process that and its awaitable finishes that it
        completes correctly when played again.
        """
        proc = self.loop.create(WaitForSignalProcess)

        # Wait - Run the process and wait until it is waiting
        run_until(proc, ProcessState.WAITING, self.loop)

        proc.pause()
        proc.continue_()
        proc.play()

        # Run
        result = ~proc

        # Check it's done
        self.assertEqual(proc.state, ProcessState.STOPPED)
        self.assertTrue(proc.has_finished())

    def test_wait_pause_continue_tick_play(self):
        """
        Test that if you pause a process that and its awaitable finishes that it
        completes correctly when played again.
        """
        proc = self.loop.create(WaitForSignalProcess)

        # Wait - Run the process and wait until it is waiting
        run_until(proc, ProcessState.WAITING, self.loop)

        proc.pause()
        proc.continue_()
        self.loop.tick()  # This should schedule the awaitable done callback
        self.loop.tick()  # Now the callback should be processed
        proc.play()

        # Run
        result = ~proc

        # Check it's done
        self.assertEqual(proc.state, ProcessState.STOPPED)
        self.assertTrue(proc.has_finished())

    def test_wait_save_continue(self):
        """ Test that process saved while in WAITING state restarts correctly when loaded """
        proc = self.loop.create(WaitForSignalProcess)

        # Wait - Run the process until it enters the WAITING state
        run_until(proc, ProcessState.WAITING, self.loop)

        saved_state = apricotpy.persistable.Bundle(proc)

        # Run the process to the end
        proc.continue_()
        result = ~proc

        # Load from saved state and run again
        proc = saved_state.unbundle(self.loop)
        proc.continue_()
        result2 = ~proc

        # Check results match
        self.assertEqual(result, result2)

    def _check_process_against_snapshot(self, snapshot, proc):
        self.assertEqual(snapshot.state, proc.state)

        new_bundle = apricotpy.Bundle()
        proc.save_instance_state(new_bundle)
        self.assertEqual(snapshot.bundle, new_bundle,
                         "Bundle mismatch with process class {}\n"
                         "Snapshot:\n{}\n"
                         "Loaded:\n{}".format(
                             proc.__class__, snapshot.bundle, new_bundle))

        self.assertEqual(snapshot.outputs, proc.outputs,
                         "Outputs mismatch with process class {}\n"
                         "Snapshot:\n{}\n"
                         "Loaded:\n{}".format(
                             proc.__class__, snapshot.outputs, proc.outputs))


class TestProcessEvents(TestCase):
    def setUp(self):
        super(TestProcessEvents, self).setUp()

        self.loop = loop_factory()

        self.events_tester = ProcessListenerTester()
        self.proc = self.loop.create(DummyProcessWithOutput)
        self.proc.add_process_listener(self.events_tester)

    def tearDown(self):
        self.proc.remove_process_listener(self.events_tester)
        super(TestProcessEvents, self).tearDown()

    def test_on_start(self):
        self.loop.run_until_complete(self.proc)
        self.assertTrue(self.events_tester.start)

    def test_on_run(self):
        self.loop.run_until_complete(self.proc)
        self.assertTrue(self.events_tester.run)

    def test_on_output_emitted(self):
        self.loop.run_until_complete(self.proc)
        self.assertTrue(self.events_tester.emitted)

    def test_on_finished(self):
        self.loop.run_until_complete(self.proc)
        self.assertTrue(self.events_tester.finish)

    def test_events_run_through(self):
        self.loop.run_until_complete(self.proc)
        self.assertTrue(self.events_tester.start)
        self.assertTrue(self.events_tester.run)
        self.assertTrue(self.events_tester.emitted)
        self.assertTrue(self.events_tester.finish)
        self.assertTrue(self.events_tester.stop)
        self.assertTrue(self.events_tester.terminate)


class _RestartProcess(WaitForSignalProcess):
    @classmethod
    def define(cls, spec):
        super(_RestartProcess, cls).define(spec)
        spec.dynamic_output()

    def finish(self):
        self.out("finished", True)


class TestExposeProcess(TestCase):

    def setUp(self):
        super(TestExposeProcess, self).setUp()

        class SimpleProcess(Process):
            @classmethod
            def define(cls, spec):
                super(SimpleProcess, cls).define(spec)
                spec.input('a', valid_type=int, required=True)
                spec.input('b', valid_type=int, required=True)

            @override
            def _run(self, **kwargs):
                pass

        self.loop = loop_factory()
        self.SimpleProcess = SimpleProcess

    def test_expose_duplicate_unnamespaced(self):
        """
        As long as separate namespaces are used, the same Process should be
        able to be exposed more than once
        """
        loop = self.loop
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess)
                spec.expose_inputs(SimpleProcess, namespace='beta')

            @override
            def _run(self, **kwargs):
                assert 'a' in self.inputs
                assert 'b' in self.inputs
                assert 'a' in self.inputs.beta
                assert 'b' in self.inputs.beta
                assert self.inputs['a'] == 1
                assert self.inputs['b'] == 2
                assert self.inputs.beta['a'] == 3
                assert self.inputs.beta['b'] == 4
                loop.create(SimpleProcess, self.exposed_inputs(SimpleProcess))
                loop.create(SimpleProcess, self.exposed_inputs(SimpleProcess, namespace='beta'))

        loop_object = loop.create(ExposeProcess, {'a': 1, 'b': 2, 'beta': {'a': 3, 'b': 4}})
        loop.run_until_complete(loop_object)

    def test_expose_duplicate_namespaced(self):
        """
        As long as separate namespaces are used, the same Process should be
        able to be exposed more than once
        """
        loop = self.loop
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess, namespace='alef')
                spec.expose_inputs(SimpleProcess, namespace='beta')

            @override
            def _run(self, **kwargs):
                assert 'a' in self.inputs.alef
                assert 'b' in self.inputs.alef
                assert 'a' in self.inputs.beta
                assert 'b' in self.inputs.beta
                assert self.inputs.alef['a'] == 1
                assert self.inputs.alef['b'] == 2
                assert self.inputs.beta['a'] == 3
                assert self.inputs.beta['b'] == 4
                loop.create(SimpleProcess, self.exposed_inputs(SimpleProcess, namespace='alef'))
                loop.create(SimpleProcess, self.exposed_inputs(SimpleProcess, namespace='beta'))

        loop_object = loop.create(ExposeProcess, {'alef': {'a': 1, 'b': 2}, 'beta': {'a': 3, 'b': 4}})
        loop.run_until_complete(loop_object)

    def test_expose_pass_same_dictionary(self):
        """
        Pass the same dictionary to two different namespaces.
        """
        loop = self.loop
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess, namespace='alef')
                spec.expose_inputs(SimpleProcess, namespace='beta')

            @override
            def _run(self, **kwargs):
                assert 'a' in self.inputs.alef
                assert 'b' in self.inputs.alef
                assert 'a' in self.inputs.beta
                assert 'b' in self.inputs.beta
                assert self.inputs.alef['a'] == 1
                assert self.inputs.alef['b'] == 2
                assert self.inputs.beta['a'] == 1
                assert self.inputs.beta['b'] == 2
                loop.create(SimpleProcess, self.exposed_inputs(SimpleProcess, namespace='alef'))
                loop.create(SimpleProcess, self.exposed_inputs(SimpleProcess, namespace='beta'))

        inputs = {'a': 1, 'b': 2}
        loop_object = loop.create(ExposeProcess, {'alef': inputs, 'beta': inputs})
        loop.run_until_complete(loop_object)


class TestNestedUnnamespacedExposedProcess(TestCase):

    def setUp(self):
        super(TestNestedUnnamespacedExposedProcess, self).setUp()

        loop = loop_factory()

        class BaseProcess(Process):
            @classmethod
            def define(cls, spec):
                super(BaseProcess, cls).define(spec)
                spec.input('a', valid_type=int, required=True)
                spec.input('b', valid_type=int, default=0)

            @override
            def _run(self, **kwargs):
                pass

        class SubProcess(Process):
            @classmethod
            def define(cls, spec):
                super(SubProcess, cls).define(spec)
                spec.expose_inputs(BaseProcess)
                spec.input('c', valid_type=int, required=True)
                spec.input('d', valid_type=int, default=0)

            @override
            def _run(self, **kwargs):
                loop_object = loop.create(BaseProcess, self.exposed_inputs(BaseProcess))
                loop.run_until_complete(loop_object)

        class ParentProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ParentProcess, cls).define(spec)
                spec.expose_inputs(SubProcess)
                spec.input('e', valid_type=int, required=True)
                spec.input('f', valid_type=int, default=0)

            @override
            def _run(self, **kwargs):
                loop_object = loop.create(SubProcess, self.exposed_inputs(SubProcess))
                loop.run_until_complete(loop_object)

        self.loop = loop
        self.BaseProcess = BaseProcess
        self.SubProcess = SubProcess
        self.ParentProcess = ParentProcess

    def test_base_process_valid_input(self):
        loop_object = self.loop.create(self.BaseProcess,
            {'a': 0, 'b': 1}
        )
        self.loop.run_until_complete(loop_object)

    def test_sub_process_valid_input(self):
        loop_object = self.loop.create(self.SubProcess,
            {'a': 0, 'b': 1, 'c': 2, 'd': 3}
        )
        self.loop.run_until_complete(loop_object)

    def test_parent_process_valid_input(self):
        loop_object = self.loop.create(self.ParentProcess,
            {'a': 0, 'b': 1, 'c': 2, 'd': 3, 'e': 4, 'f': 5}
        )
        self.loop.run_until_complete(loop_object)

    def test_base_process_missing_input(self):
        with self.assertRaises(ValueError):
            loop_object = self.loop.create(self.BaseProcess,
                {'b': 1}
            )
            self.loop.run_until_complete(loop_object)

    def test_sub_process_missing_input(self):
        with self.assertRaises(ValueError):
            loop_object = self.loop.create(self.SubProcess,
                {'b': 1}
            )
            self.loop.run_until_complete(loop_object)

    def test_parent_process_missing_input(self):
        with self.assertRaises(ValueError):
            loop_object = self.loop.create(self.ParentProcess,
                {'b': 1}
            )
            self.loop.run_until_complete(loop_object)

    def test_base_process_invalid_type_input(self):
        with self.assertRaises(ValueError):
            loop_object = self.loop.create(self.BaseProcess,
                {'a': 0, 'b': 'string'}
            )
            self.loop.run_until_complete(loop_object)

    def test_sub_process_invalid_type_input(self):
        with self.assertRaises(ValueError):
            loop_object = self.loop.create(self.SubProcess,
                {'a': 0, 'b': 1, 'c': 2, 'd': 'string'}
            )
            self.loop.run_until_complete(loop_object)

    def test_parent_process_invalid_type_input(self):
        with self.assertRaises(ValueError):
            loop_object = self.loop.create(self.ParentProcess,
                {'a': 0, 'b': 1, 'c': 2, 'd': 3, 'e': 4, 'f': 'string'}
            )
            self.loop.run_until_complete(loop_object)


class TestNestedNamespacedExposedProcess(TestCase):

    def setUp(self):
        super(TestNestedNamespacedExposedProcess, self).setUp()

        loop = loop_factory()

        class BaseProcess(Process):
            @classmethod
            def define(cls, spec):
                super(BaseProcess, cls).define(spec)
                spec.input('a', valid_type=int, required=True)
                spec.input('b', valid_type=int, default=0)

            @override
            def _run(self, **kwargs):
                pass

        class SubProcess(Process):
            @classmethod
            def define(cls, spec):
                super(SubProcess, cls).define(spec)
                spec.expose_inputs(BaseProcess, namespace='base')
                spec.input('c', valid_type=int, required=True)
                spec.input('d', valid_type=int, default=0)

            @override
            def _run(self, **kwargs):
                loop_object = loop.create(BaseProcess, self.exposed_inputs(BaseProcess, namespace='base'))
                loop.run_until_complete(loop_object)

        class ParentProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ParentProcess, cls).define(spec)
                spec.expose_inputs(SubProcess, namespace='sub')
                spec.input('e', valid_type=int, required=True)
                spec.input('f', valid_type=int, default=0)

            @override
            def _run(self, **kwargs):
                loop_object = loop.create(SubProcess, self.exposed_inputs(SubProcess, namespace='sub'))
                loop.run_until_complete(loop_object)

        self.loop = loop
        self.BaseProcess = BaseProcess
        self.SubProcess = SubProcess
        self.ParentProcess = ParentProcess

    def test_sub_process_valid_input(self):
        loop_object = self.loop.create(self.SubProcess,
            {'base': {'a': 0, 'b': 1}, 'c': 2}
        )
        self.loop.run_until_complete(loop_object)

    def test_parent_process_valid_input(self):
        loop_object = self.loop.create(self.ParentProcess,
            {'sub': {'base': {'a': 0, 'b': 1}, 'c': 2}, 'e': 4}
        )
        self.loop.run_until_complete(loop_object)

    def test_sub_process_missing_input(self):
        with self.assertRaises(ValueError):
            loop_object = self.loop.create(self.SubProcess, {'c': 2})
            self.loop.run_until_complete(loop_object)

    def test_parent_process_missing_input(self):
        with self.assertRaises(ValueError):
            loop_object = self.loop.create(self.ParentProcess, {'e': 4})
            self.loop.run_until_complete(loop_object)


class TestExcludeExposeProcess(TestCase):

    def setUp(self):
        super(TestExcludeExposeProcess, self).setUp()

        class SimpleProcess(Process):
            @classmethod
            def define(cls, spec):
                super(SimpleProcess, cls).define(spec)
                spec.input('a', valid_type=int, required=True)
                spec.input('b', valid_type=int, required=False)

            @override
            def _run(self, **kwargs):
                pass

        self.loop = loop_factory()
        self.SimpleProcess = SimpleProcess

    def test_exclude_valid(self):
        loop = self.loop
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess, exclude=('b',))
                spec.input('c', valid_type=int, required=True)

            @override
            def _run(self, **kwargs):
                loop.create(SimpleProcess, self.exposed_inputs(SimpleProcess))

        loop_object = loop.create(ExposeProcess, {'a': 1, 'c': 3})
        loop.run_until_complete(loop_object)

    def test_exclude_invalid(self):
        loop = self.loop
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess, exclude=('a',))

            @override
            def _run(self, **kwargs):
                loop.create(SimpleProcess, self.exposed_inputs(SimpleProcess))

        with self.assertRaises(ValueError):
            loop_object = loop.create(ExposeProcess, {'b': 2, 'c': 3})
            loop.run_until_complete(loop_object)


class TestIncludeExposeProcess(TestCase):

    def setUp(self):
        super(TestIncludeExposeProcess, self).setUp()

        class SimpleProcess(Process):
            @classmethod
            def define(cls, spec):
                super(SimpleProcess, cls).define(spec)
                spec.input('a', valid_type=Int, required=True)
                spec.input('b', valid_type=Int, required=False)

            @override
            def _run(self, **kwargs):
                pass

        self.SimpleProcess = SimpleProcess

    def test_include_none(self):
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess, include=[])
                spec.input('c', valid_type=Int, required=True)

            @override
            def _run(self, **kwargs):
                loop.create(SimpleProcess, a=Int(1), b=Int(2), self.exposed_inputs(SimpleProcess))

    def test_include_one(self):
        SimpleProcess = self.SimpleProcess

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SimpleProcess, include=['a'])
                spec.input('c', valid_type=Int, required=True)

            @override
            def _run(self, **kwargs):
                loop.create(SimpleProcess, b=Int(2), self.exposed_inputs(SimpleProcess))

        loop.create(ExposeProcess, **{'a': Int(1), 'c': Int(3)})
 

class TestUnionInputsExposeProcess(TestCase):

    def setUp(self):
        super(TestUnionInputsExposeProcess, self).setUp()

        loop = loop_factory()

        class SubOneProcess(Process):
            @classmethod
            def define(cls, spec):
                super(SubOneProcess, cls).define(spec)
                spec.input('common', valid_type=int, required=True)
                spec.input('sub_one', valid_type=int, required=True)

            @override
            def _run(self, **kwargs):
                assert self.inputs['common'] == 1
                assert self.inputs['sub_one'] == 2

        class SubTwoProcess(Process):
            @classmethod
            def define(cls, spec):
                super(SubTwoProcess, cls).define(spec)
                spec.input('common', valid_type=int, required=True)
                spec.input('sub_two', valid_type=int, required=True)

            @override
            def _run(self, **kwargs):
                assert self.inputs['common'] == 1
                assert self.inputs['sub_two'] == 3

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SubOneProcess)
                spec.expose_inputs(SubTwoProcess)

            @override
            def _run(self, **kwargs):
                loop_object = loop.create(SubOneProcess, self.exposed_inputs(SubOneProcess))
                loop.run_until_complete(loop_object)
                loop_object = loop.create(SubTwoProcess, self.exposed_inputs(SubTwoProcess))
                loop.run_until_complete(loop_object)

        self.loop = loop
        self.SubOneProcess = SubOneProcess
        self.SubTwoProcess = SubTwoProcess
        self.ExposeProcess = ExposeProcess

    def test_inputs_union_valid(self):
        loop_object = self.loop.create(self.ExposeProcess, {'common': 1, 'sub_one': 2, 'sub_two': 3})
        self.loop.run_until_complete(loop_object)

    def test_inputs_union_invalid(self):
        with self.assertRaises(ValueError):
            loop_object = self.loop.create(self.ExposeProcess, {'sub_one': 2, 'sub_two': 3})
            self.loop.run_until_complete(loop_object)


class TestAgglomerateExposeProcess(TestCase):
    """
    Often one wants to run multiple instances of a certain Process, where some but
    not all the inputs will be the same or "common". By using a combination of include and
    exclude on the same Process, the user can define separate namespaces for the specific
    inputs, while exposing the shared or common inputs on the base level namespace. The
    method exposed_inputs will by default agglomerate inputs that belong to the SubProcess
    starting from the base level and moving down the specified namespaces, overriding duplicate
    inputs as they are found.

    The exposed_inputs provides the flag 'agglomerate' which can be set to False to turn off
    this behavior and only return the inputs in the specified namespace
    """

    def setUp(self):
        super(TestAgglomerateExposeProcess, self).setUp()

        loop = loop_factory()

        class SubProcess(Process):
            @classmethod
            def define(cls, spec):
                super(SubProcess, cls).define(spec)
                spec.input('common', valid_type=int, required=True)
                spec.input('specific_a', valid_type=int, required=True)
                spec.input('specific_b', valid_type=int, required=True)

            @override
            def _run(self, **kwargs):
                pass

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SubProcess, include=('common,'))
                spec.expose_inputs(SubProcess, namespace='sub_a', exclude=('common,'))
                spec.expose_inputs(SubProcess, namespace='sub_b', exclude=('common,'))

            @override
            def _run(self, **kwargs):
                loop_object = loop.create(SubProcess, self.exposed_inputs(SubProcess, namespace='sub_a'))
                loop.run_until_complete(loop_object)
                loop_object = loop.create(SubProcess, self.exposed_inputs(SubProcess, namespace='sub_b'))
                loop.run_until_complete(loop_object)

        self.loop = loop
        self.SubProcess = SubProcess
        self.ExposeProcess = ExposeProcess

    def test_inputs_union_valid(self):
        inputs = {
            'common': 1,
            'sub_a': {
                'specific_a': 2,
                'specific_b': 3
            },
            'sub_b': {
                'specific_a': 4,
                'specific_b': 5
            }
        }
        loop_object = self.loop.create(self.ExposeProcess, inputs)
        self.loop.run_until_complete(loop_object)

    def test_inputs_union_invalid(self):
        inputs = {
            'sub_a': {
                'specific_a': 2,
                'specific_b': 3
            },
            'sub_b': {
                'specific_a': 4,
                'specific_b': 5
            }
        }
        with self.assertRaises(ValueError):
            loop_object = self.loop.create(self.ExposeProcess, inputs)
            self.loop.run_until_complete(loop_object)


class TestNonAgglomerateExposeProcess(TestCase):
    """
    Example where the default agglomerate behavior of exposed_inputs is undesirable and can be
    switched off by setting the flag agglomerate to False. The SubProcess shares an input with
    the parent processs, but unlike for the ExposeProcess, for the SubProcess it is not required.
    A user might for that reason not want to pass the common input to the SubProcess.
    """

    def setUp(self):
        super(TestNonAgglomerateExposeProcess, self).setUp()

        loop = loop_factory()

        class SubProcess(Process):
            @classmethod
            def define(cls, spec):
                super(SubProcess, cls).define(spec)
                spec.input('specific_a', valid_type=int, required=True)
                spec.input('specific_b', valid_type=int, required=True)
                spec.input('common', valid_type=int, required=False)

            @override
            def _run(self, **kwargs):
                assert 'common' not in self.inputs

        class ExposeProcess(Process):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(SubProcess, namespace='sub')
                spec.input('common', valid_type=int, required=True)

            @override
            def _run(self, **kwargs):
                loop_object = loop.create(SubProcess, self.exposed_inputs(SubProcess, namespace='sub', agglomerate=False))
                loop.run_until_complete(loop_object)

        self.loop = loop
        self.SubProcess = SubProcess
        self.ExposeProcess = ExposeProcess

    def test_valid_input_non_agglomerate(self):
        inputs = {
            'common': 1,
            'sub': {
                'specific_a': 2,
                'specific_b': 3
            },
        }
        loop_object = self.loop.create(self.ExposeProcess, inputs)
        self.loop.run_until_complete(loop_object)