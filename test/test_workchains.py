import inspect
from past.builtins import basestring
import plumpy
from plumpy.workchains import *
import unittest

from plumpy import test_utils
from . import utils


class Wf(WorkChain):
    # Keep track of which steps were completed by the workflow
    finished_steps = {}

    @classmethod
    def define(cls, spec):
        super(Wf, cls).define(spec)
        spec.input("value", default='A')
        spec.input("n", default=3)
        spec.outputs.dynamic = True
        spec.outline(
            cls.s1,
            if_(cls.isA)(
                cls.s2
            ).elif_(cls.isB)(
                cls.s3
            ).else_(
                cls.s4
            ),
            cls.s5,
            while_(cls.ltN)(
                cls.s6
            ),
        )

    def on_create(self):
        super(Wf, self).on_create()
        # Reset the finished step
        self.finished_steps = {
            k: False for k in
            [self.s1.__name__, self.s2.__name__, self.s3.__name__,
             self.s4.__name__, self.s5.__name__, self.s6.__name__,
             self.isA.__name__, self.isB.__name__, self.ltN.__name__]
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

    def isA(self):
        self._set_finished(inspect.stack()[0][3])
        return self.inputs.value == 'A'

    def isB(self):
        self._set_finished(inspect.stack()[0][3])
        return self.inputs.value == 'B'

    def ltN(self):
        keep_looping = self.ctx.counter < self.inputs.n
        if not keep_looping:
            self._set_finished(inspect.stack()[0][3])
        return keep_looping

    def _set_finished(self, function_name):
        self.finished_steps[function_name] = True


class IfTest(WorkChain):
    @classmethod
    def define(cls, spec):
        super(IfTest, cls).define(spec)
        spec.outline(
            if_(cls.condition)(
                cls.step1,
                cls.step2
            )
        )

    def on_create(self, *args, **kwargs):
        super(IfTest, self).on_create(*args, **kwargs)
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
        super(DummyWc, cls).define(spec)
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


class TestWorkchain(utils.TestCaseWithLoop):
    def test_run(self):
        A = 'A'
        B = 'B'
        C = 'C'
        three = 3

        # Try the if(..) part
        Wf(inputs=dict(value=A, n=three)).execute()
        # Check the steps that should have been run
        for step, finished in Wf.finished_steps.items():
            if step not in ['s3', 's4', 'isB']:
                self.assertTrue(finished, "Step {} was not called by workflow".format(step))

        # Try the elif(..) part
        finished_steps = Wf(inputs=dict(value=B, n=three)).execute()
        # Check the steps that should have been run
        for step, finished in finished_steps.items():
            if step not in ['isA', 's2', 's4']:
                self.assertTrue(finished, "Step {} was not called by workflow".format(step))

        # Try the else... part
        finished_steps = Wf(inputs=dict(value=C, n=three)).execute()
        # Check the steps that should have been run
        for step, finished in finished_steps.items():
            if step not in ['isA', 's2', 'isB', 's3']:
                self.assertTrue(finished, "Step {} was not called by workflow".format(step))

    def test_incorrect_outline(self):
        class Wf(WorkChain):
            @classmethod
            def define(cls, spec):
                super(Wf, cls).define(spec)
                # Try defining an invalid outline
                spec.outline(5)

        with self.assertRaises(TypeError):
            Wf.spec()

    def test_same_input_node(self):
        class Wf(WorkChain):
            @classmethod
            def define(cls, spec):
                super(Wf, cls).define(spec)
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
        A = "a"
        B = "b"

        class ReturnA(plumpy.Process):
            @classmethod
            def define(cls, spec):
                super(ReturnA, cls).define(spec)
                spec.output('res')

            def run(self):
                self.out('res', A)

        class ReturnB(plumpy.Process):
            @classmethod
            def define(cls, spec):
                super(ReturnB, cls).define(spec)
                spec.output('res')

            def run(self):
                self.out('res', B)

        class Wf(WorkChain):
            @classmethod
            def define(cls, spec):
                super(Wf, cls).define(spec)
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
        self.assertIsInstance(str(Wf.spec()), basestring)

    def test_malformed_outline(self):
        """
        Test some malformed outlines
        """
        spec = _WorkChainSpec()

        with self.assertRaises(TypeError):
            spec.outline(5)

        with self.assertRaises(TypeError):
            spec.outline(lambda x, y: 5)

    def test_checkpointing(self):
        A = 'A'
        B = 'B'
        C = 'C'
        three = 3

        # Try the if(..) part
        finished_steps = self._run_with_checkpoints(Wf, inputs={'value': A, 'n': three})
        # Check the steps that should have been run
        for step, finished in finished_steps.items():
            if step not in ['s3', 's4', 'isB']:
                self.assertTrue(finished, "Step {} was not called by workflow".format(step))

        # Try the elif(..) part
        finished_steps = self._run_with_checkpoints(Wf, inputs={'value': B, 'n': three})
        # Check the steps that should have been run
        for step, finished in finished_steps.items():
            if step not in ['isA', 's2', 's4']:
                self.assertTrue(finished, "Step {} was not called by workflow".format(step))

        # Try the else... part
        finished_steps = self._run_with_checkpoints(Wf, inputs={'value': C, 'n': three})
        # Check the steps that should have been run
        for step, finished in finished_steps.items():
            if step not in ['isA', 's2', 'isB', 's3']:
                self.assertTrue(finished, "Step {} was not called by workflow".format(step))

    def test_return(self):
        class WcWithReturn(WorkChain):
            @classmethod
            def define(cls, spec):
                super(WcWithReturn, cls).define(spec)
                spec.outline(
                    cls.s1,
                    if_(cls.isA)(
                        return_
                    ),
                    cls.after
                )

            def s1(self):
                pass

            def isA(self):
                return True

            def after(self):
                raise RuntimeError("Shouldn't get here")

        workchain = WcWithReturn()
        workchain.execute()

    def test_tocontext_schedule_workchain(self):
        class MainWorkChain(WorkChain):
            @classmethod
            def define(cls, spec):
                super(MainWorkChain, cls).define(spec)
                spec.outline(cls.run, cls.check)
                spec.outputs.dynamic = True

            def run(self):
                return ToContext(subwc=self.launch(SubWorkChain))

            def check(self):
                assert self.ctx.subwc.out.value == 5

        class SubWorkChain(WorkChain):
            @classmethod
            def define(cls, spec):
                super(SubWorkChain, cls).define(spec)
                spec.outline(cls.run)

            def run(self):
                self.out("value", 5)

        workchain = MainWorkChain()
        workchain.execute()

    def test_if_block_persistence(self):
        wc = IfTest()
        wc.execute(True)
        self.assertTrue(wc.ctx.s1)
        self.assertFalse(wc.ctx.s2)

        # Now bundle the thing
        bundle = plumpy.Bundle(wc)

        # Load from saved tate
        wc = bundle.unbundle()
        self.assertTrue(wc.ctx.s1)
        self.assertFalse(wc.ctx.s2)

        bundle2 = plumpy.Bundle(wc)
        self.assertDictEqual(bundle, bundle2)

        wc.execute()
        self.assertTrue(wc.ctx.s1)
        self.assertTrue(wc.ctx.s2)

    def test_to_context(self):
        val = 5

        class SimpleWc(plumpy.Process):
            @classmethod
            def define(cls, spec):
                super(SimpleWc, cls).define(spec)
                spec.output("_return")

            def run(self):
                self.out('_return', val)

        class Workchain(WorkChain):
            @classmethod
            def define(cls, spec):
                super(Workchain, cls).define(spec)
                spec.outline(cls.begin, cls.check)

            def begin(self):
                self.to_context(result_a=self.launch(SimpleWc))
                return ToContext(result_b=self.launch(SimpleWc))

            def check(self):
                assert self.ctx.result_a['_return'] == val
                assert self.ctx.result_b['_return'] == val

        workchain = Workchain()
        workchain.execute()

    # def test_persisting(self):
    #     persister = plumpy.test_utils.TestPersister()
    #     runner = work.new_runner(persister=persister)
    #     workchain = Wf(runner=runner)
    #     workchain.execute()

    def test_exception_tocontext(self):
        my_exception = RuntimeError("Should not be reached")

        class Workchain(WorkChain):
            @classmethod
            def define(cls, spec):
                super(Workchain, cls).define(spec)
                spec.outline(cls.begin, cls.check)

            def begin(self):
                self.to_context(result_a=self.launch(test_utils.ExceptionProcess))

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


class TestImmutableInputWorkchain(utils.TestCaseWithLoop):
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
                super(Wf, cls).define(spec)
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
                test_class.assertEquals(self.inputs['a'], 1)

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
                super(Wf, cls).define(spec)
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
                test_class.assertEquals(self.inputs.subspace['one'], 1)

        workchain = Wf(inputs=dict(subspace={'one': 1, 'two': 2}))
        workchain.execute()
