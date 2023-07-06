# -*- coding: utf-8 -*-
import asyncio
import inspect
import unittest

import pytest

import plumpy
from plumpy.process_listener import ProcessListener
from plumpy.workchains import *

from . import utils


class Wf(WorkChain):
    # Keep track of which steps were completed by the workflow
    finished_steps = {}

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input('value', default='A')
        spec.input('n', default=3)
        spec.outputs.dynamic = True
        spec.outline(
            cls.s1,
            if_(cls.isA)(cls.s2).elif_(cls.isB)(cls.s3).else_(cls.s4),
            cls.s5,
            while_(cls.ltN)(cls.s6),
        )

    def on_create(self):
        super().on_create()
        # Reset the finished step
        self.finished_steps = {
            k: False
            for k in [
                self.s1.__name__,
                self.s2.__name__,
                self.s3.__name__,
                self.s4.__name__,
                self.s5.__name__,
                self.s6.__name__,
                self.isA.__name__,
                self.isB.__name__,
                self.ltN.__name__,
            ]
        }

    def s1(self):
        self._set_finished(inspect.stack()[0][3])

    def s2(self):
        self._set_finished(inspect.stack()[0][3])

    def s3(self):
        self._set_finished(inspect.stack()[0][3])

    def s4(self):
        self._set_finished(inspect.stack()[0][3])

    def s5(self):
        self.ctx.counter = 0
        self._set_finished(inspect.stack()[0][3])

    def s6(self):
        self.ctx.counter = self.ctx.counter + 1
        self._set_finished(inspect.stack()[0][3])

    def isA(self):  # noqa: N802
        self._set_finished(inspect.stack()[0][3])
        return self.inputs.value == 'A'

    def isB(self):  # noqa: N802
        self._set_finished(inspect.stack()[0][3])
        return self.inputs.value == 'B'

    def ltN(self):  # noqa: N802
        keep_looping = self.ctx.counter < self.inputs.n
        if not keep_looping:
            self._set_finished(inspect.stack()[0][3])
        return keep_looping

    def _set_finished(self, function_name):
        self.finished_steps[function_name] = True


class IfTest(WorkChain):
    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.outline(if_(cls.condition)(cls.step1, cls.step2))

    def on_create(self, *args, **kwargs):
        super().on_create(*args, **kwargs)
        self.ctx.s1 = False
        self.ctx.s2 = False

    def condition(self):
        return True

    def step1(self):
        self.ctx.s1 = True
        self.pause()

    def step2(self):
        self.ctx.s2 = True


class DummyWc(WorkChain):
    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.outline(cls.do_nothing)

    def do_nothing(self):
        pass


class TestContext(unittest.TestCase):
    def test_attributes(self):
        wc = DummyWc()
        wc.ctx.new_attr = 5
        self.assertEqual(wc.ctx.new_attr, 5)

        del wc.ctx.new_attr
        with self.assertRaises(AttributeError):
            wc.ctx.new_attr

    def test_dict(self):
        wc = DummyWc()
        wc.ctx['new_attr'] = 5
        self.assertEqual(wc.ctx['new_attr'], 5)

        del wc.ctx['new_attr']
        with self.assertRaises(KeyError):
            wc.ctx['new_attr']


class TestWorkchain(unittest.TestCase):
    maxDiff = None

    def test_run(self):
        A = 'A'  # noqa: N806
        B = 'B'  # noqa: N806
        C = 'C'  # noqa: N806
        three = 3

        # Try the if(..) part
        Wf(inputs=dict(value=A, n=three)).execute()
        # Check the steps that should have been run
        for step, finished in Wf.finished_steps.items():
            if step not in ['s3', 's4', 'isB']:
                self.assertTrue(finished, f'Step {step} was not called by workflow')

        # Try the elif(..) part
        finished_steps = Wf(inputs=dict(value=B, n=three)).execute()
        # Check the steps that should have been run
        for step, finished in finished_steps.items():
            if step not in ['isA', 's2', 's4']:
                self.assertTrue(finished, f'Step {step} was not called by workflow')

        # Try the else... part
        finished_steps = Wf(inputs=dict(value=C, n=three)).execute()
        # Check the steps that should have been run
        for step, finished in finished_steps.items():
            if step not in ['isA', 's2', 'isB', 's3']:
                self.assertTrue(finished, f'Step {step} was not called by workflow')

    def test_incorrect_outline(self):
        class Wf(WorkChain):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                # Try defining an invalid outline
                spec.outline(5)

        with self.assertRaises(TypeError):
            Wf.spec()

    def test_same_input_node(self):
        class Wf(WorkChain):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('a', valid_type=int)
                spec.input('b', valid_type=int)
                # Try defining an invalid outline
                spec.outline(cls.check_a_b)

            def check_a_b(self):
                assert 'a' in self.inputs
                assert 'b' in self.inputs

        x = 1
        Wf(inputs=dict(a=x, b=x)).execute()

    def test_context(self):
        A = 'a'  # noqa: N806
        B = 'b'  # noqa: N806

        class ReturnA(plumpy.Process):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.output('res')

            async def run(self):
                self.out('res', A)

        class ReturnB(plumpy.Process):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.output('res')

            async def run(self):
                self.out('res', B)

        class Wf(WorkChain):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.outline(cls.s1, cls.s2, cls.s3)

            def s1(self):
                return ToContext(r1=self.launch(ReturnA), r2=self.launch(ReturnB))

            def s2(self):
                assert self.ctx.r1['res'] == A
                assert self.ctx.r2['res'] == B

                # Try overwriting r1
                return ToContext(r1=self.launch(ReturnB))

            def s3(self):
                assert self.ctx.r1['res'] == B
                assert self.ctx.r2['res'] == B

        Wf().execute()

    def test_str(self):
        self.assertIsInstance(str(Wf.spec()), str)

    def test_malformed_outline(self):
        """
        Test some malformed outlines
        """
        spec = WorkChainSpec()

        with self.assertRaises(TypeError):
            spec.outline(5)

        with self.assertRaises(TypeError):
            spec.outline(lambda x, y: 5)

    def test_checkpointing(self):
        A = 'A'  # noqa: N806
        B = 'B'  # noqa: N806
        C = 'C'  # noqa: N806
        three = 3

        # Try the if(..) part
        finished_steps = self._run_with_checkpoints(Wf, inputs={'value': A, 'n': three})
        # Check the steps that should have been run
        for step, finished in finished_steps.items():
            if step not in ['s3', 's4', 'isB']:
                self.assertTrue(finished, f'Step {step} was not called by workflow')

        # Try the elif(..) part
        finished_steps = self._run_with_checkpoints(Wf, inputs={'value': B, 'n': three})
        # Check the steps that should have been run
        for step, finished in finished_steps.items():
            if step not in ['isA', 's2', 's4']:
                self.assertTrue(finished, f'Step {step} was not called by workflow')

        # Try the else... part
        finished_steps = self._run_with_checkpoints(Wf, inputs={'value': C, 'n': three})
        # Check the steps that should have been run
        for step, finished in finished_steps.items():
            if step not in ['isA', 's2', 'isB', 's3']:
                self.assertTrue(finished, f'Step {step} was not called by workflow')

    def test_listener_persistence(self):
        persister = plumpy.InMemoryPersister()
        process_finished_count = 0

        class TestListener(plumpy.ProcessListener):
            def on_process_finished(self, process, output):
                nonlocal process_finished_count
                process_finished_count += 1

        class SimpleWorkChain(plumpy.WorkChain):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.outline(
                    cls.step1,
                    cls.step2,
                )

            def step1(self):
                persister.save_checkpoint(self, 'step1')

            def step2(self):
                persister.save_checkpoint(self, 'step2')

        # add SimpleWorkChain and TestListener to this module global namespace, so they can be reloaded from checkpoint
        globals()['SimpleWorkChain'] = SimpleWorkChain
        globals()['TestListener'] = TestListener

        workchain = SimpleWorkChain()
        workchain.add_process_listener(TestListener())

        workchain.execute()

        self.assertEqual(process_finished_count, 1)

        workchain_checkpoint = persister.load_checkpoint(workchain.pid, 'step1').unbundle()
        workchain_checkpoint.execute()
        self.assertEqual(process_finished_count, 2)

    def test_return_in_outline(self):
        class WcWithReturn(WorkChain):
            FAILED_CODE = 1

            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('success', valid_type=bool, required=False)
                spec.outline(
                    cls.step_one,
                    if_(cls.do_success)(return_).elif_(cls.do_failed)(return_(cls.FAILED_CODE)).else_(cls.default),
                )

            def step_one(self):
                pass

            def do_success(self):
                return 'success' in self.inputs and self.inputs.success is True

            def do_failed(self):
                return 'success' in self.inputs and self.inputs.success is False

            def default(self):
                raise RuntimeError('Should already have returned')

        workchain = WcWithReturn(inputs=dict(success=True))
        workchain.execute()

        workchain = WcWithReturn(inputs=dict(success=False))
        workchain.execute()

        with self.assertRaises(RuntimeError):
            workchain = WcWithReturn()
            workchain.execute()

    def test_return_in_step(self):
        class WcWithReturn(WorkChain):
            FAILED_CODE = 1

            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('success', valid_type=bool, required=False)
                spec.outline(cls.step_one, cls.after)

            def step_one(self):
                if 'success' not in self.inputs:
                    return
                elif self.inputs.success is True:
                    return 0
                elif self.inputs.success is False:
                    return self.FAILED_CODE

            def after(self):
                raise RuntimeError('Should already have returned')

        workchain = WcWithReturn(inputs=dict(success=True))
        workchain.execute()

        workchain = WcWithReturn(inputs=dict(success=False))
        workchain.execute()

        with self.assertRaises(RuntimeError):
            workchain = WcWithReturn()
            workchain.execute()

    def test_tocontext_schedule_workchain(self):
        class MainWorkChain(WorkChain):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.outline(cls.run, cls.check)
                spec.outputs.dynamic = True

            async def run(self):
                return ToContext(subwc=self.launch(SubWorkChain))

            def check(self):
                assert self.ctx.subwc.out.value == 5

        class SubWorkChain(WorkChain):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.outline(cls.run)

            async def run(self):
                self.out('value', 5)

        workchain = MainWorkChain()
        workchain.execute()

    def test_if_block_persistence(self):
        workchain = IfTest()

        async def async_test():
            await utils.run_until_paused(workchain)
            self.assertTrue(workchain.ctx.s1)
            self.assertFalse(workchain.ctx.s2)

            # Now bundle the thing
            bundle = plumpy.Bundle(workchain)

            # Load from saved state
            workchain2 = bundle.unbundle()
            self.assertTrue(workchain2.ctx.s1)
            self.assertFalse(workchain2.ctx.s2)

            bundle2 = plumpy.Bundle(workchain2)
            self.assertDictEqual(bundle, bundle2)

            workchain.play()
            await workchain.future()
            self.assertTrue(workchain.ctx.s1)
            self.assertTrue(workchain.ctx.s2)

        loop = asyncio.get_event_loop()
        loop.create_task(workchain.step_until_terminated())  # noqa: RUF006
        loop.run_until_complete(async_test())

    def test_to_context(self):
        val = 5

        class SimpleWc(plumpy.Process):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.output('_return')

            async def run(self):
                self.out('_return', val)

        class Workchain(WorkChain):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.outline(cls.begin, cls.check)

            def begin(self):
                self.to_context(result_a=self.launch(SimpleWc))
                return ToContext(result_b=self.launch(SimpleWc))

            def check(self):
                assert self.ctx.result_a['_return'] == val
                assert self.ctx.result_b['_return'] == val

        workchain = Workchain()
        workchain.execute()

    def test_output_namespace(self):
        """Test running a workchain with nested outputs."""

        class TestWorkChain(WorkChain):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.output('x.y', required=True)
                spec.outline(cls.do_run)

            def do_run(self):
                self.out('x.y', 5)

        workchain = TestWorkChain()
        workchain.execute()

    def test_exception_tocontext(self):
        my_exception = RuntimeError('Should not be reached')

        class Workchain(WorkChain):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.outline(cls.begin, cls.check)

            def begin(self):
                self.to_context(result_a=self.launch(utils.ExceptionProcess))

            def check(self):
                raise my_exception

        workchain = Workchain()
        with self.assertRaises(RuntimeError):
            workchain.execute()
        self.assertNotEqual(workchain.exception(), my_exception)

    def _run_with_checkpoints(self, wf_class, inputs=None):
        # TODO: Actually save at each point!
        proc = wf_class(inputs=inputs)
        proc.execute()
        return wf_class.finished_steps

    def test_stepper_info(self):
        """Check status information provided by steppers"""

        class Wf(WorkChain):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('N', valid_type=int)
                spec.outline(
                    cls.check_n,
                    while_(cls.do_step)(
                        cls.chill,
                        cls.chill,
                    ),
                    if_(cls.do_step)(
                        cls.chill,
                    )
                    .elif_(cls.do_step)(
                        cls.chill,
                    )
                    .else_(cls.chill),
                )

            def check_n(self):
                assert 'N' in self.inputs

            def chill(self):
                pass

            def do_step(self):
                if not hasattr(self.ctx, 'do_step'):
                    self.ctx.do_step = 0

                self.ctx.do_step += 1

                if self.ctx.do_step < self.inputs['N']:
                    return True
                else:
                    return False

        class StatusCollector(ProcessListener):
            def __init__(self):
                self.stepper_strings = []

            def on_process_running(self, process):
                self.stepper_strings.append(str(process._stepper))

        collector = StatusCollector()

        wf = Wf(inputs=dict(N=4))
        wf.add_process_listener(collector)
        wf.execute()

        stepper_strings = [
            '0:check_n',
            '1:while_(do_step)',
            '1:while_(do_step)(1:chill)',
            '1:while_(do_step)',
            '1:while_(do_step)(1:chill)',
            '1:while_(do_step)',
            '1:while_(do_step)(1:chill)',
            '1:while_(do_step)',
            '2:if_(do_step)',
        ]
        self.assertListEqual(collector.stepper_strings, stepper_strings)


class TestImmutableInputWorkchain(unittest.TestCase):
    """
    Test that inputs cannot be modified
    """

    def test_immutable_input(self):
        """
        Check that from within the WorkChain self.inputs returns an AttributesFrozendict which should be immutable
        """
        test_class = self

        class Wf(WorkChain):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('a', valid_type=int)
                spec.input('b', valid_type=int)
                spec.outline(
                    cls.step_one,
                    cls.step_two,
                )

            def step_one(self):
                # Attempt to manipulate the inputs dictionary which since it is a AttributesFrozendict should raise
                with test_class.assertRaises(TypeError):
                    self.inputs['a'] = 3
                with test_class.assertRaises(AttributeError):
                    self.inputs.pop('b')
                with test_class.assertRaises(TypeError):
                    self.inputs['c'] = 4

            def step_two(self):
                # Verify that original inputs are still there with same value and no inputs were added
                test_class.assertIn('a', self.inputs)
                test_class.assertIn('b', self.inputs)
                test_class.assertNotIn('c', self.inputs)
                test_class.assertEqual(self.inputs['a'], 1)

        workchain = Wf(inputs=dict(a=1, b=2))
        workchain.execute()

    def test_immutable_input_namespace(self):
        """
        Check that namespaced inputs also return AttributeFrozendicts and are hence immutable
        """
        test_class = self

        class Wf(WorkChain):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input_namespace('subspace', dynamic=True)
                spec.outline(
                    cls.step_one,
                    cls.step_two,
                )

            def step_one(self):
                # Attempt to manipulate the namespaced inputs dictionary which should raise
                with test_class.assertRaises(TypeError):
                    self.inputs.subspace['one'] = 3
                with test_class.assertRaises(AttributeError):
                    self.inputs.subspace.pop('two')
                with test_class.assertRaises(TypeError):
                    self.inputs.subspace['four'] = 4

            def step_two(self):
                # Verify that original inputs are still there with same value and no inputs were added
                test_class.assertIn('one', self.inputs.subspace)
                test_class.assertIn('two', self.inputs.subspace)
                test_class.assertNotIn('four', self.inputs.subspace)
                test_class.assertEqual(self.inputs.subspace['one'], 1)

        workchain = Wf(inputs=dict(subspace={'one': 1, 'two': 2}))
        workchain.execute()


@pytest.mark.parametrize('construct', (if_, while_))
def test_conditional_return_type(construct, recwarn):
    """Test that a conditional passed to the ``if_`` and ``while_`` functions warns for incorrect type."""

    class BoolLike:
        """Instances that implement ``__bool__`` are valid return types for conditional predicate."""

        def __bool__(self):
            return True

    def valid_conditional(self):
        return BoolLike()

    construct(valid_conditional)[0].is_true(None)
    assert len(recwarn) == 0

    def conditional_returning_none(self):
        return None

    construct(conditional_returning_none)[0].is_true(None)
    assert len(recwarn) == 0

    def invalid_conditional(self):
        return 'true'

    with pytest.warns(UserWarning):
        construct(invalid_conditional)[0].is_true(None)
