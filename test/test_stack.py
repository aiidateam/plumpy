from test.utils import TestCaseWithLoop
import plumpy


class StackTest(plumpy.Process):
    def run(self):
        assert len(plumpy.stack.stack()) == 1
        assert plumpy.stack.top() is self

    def total_ancestors(self):
        try:
            return 1 + self.inputs.parent.total_ancestors()
        except AttributeError:
            return 0


class TestStack(TestCaseWithLoop):
    def test_simple(self):
        st = StackTest().execute()

    def test_stack_push_pop(self):
        p = plumpy.Process()
        with plumpy.stack.in_stack(p):
            self.assertIs(p, plumpy.stack.top())

        self.assertTrue(plumpy.stack.is_empty())
