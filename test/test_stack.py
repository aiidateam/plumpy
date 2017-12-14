from test.util import TestCase
import plum


class StackTest(plum.Process):
    def _run(self):
        assert len(plum.stack.stack()) == 1
        assert plum.stack.top() is self

    def total_ancestors(self):
        try:
            return 1 + self.inputs.parent.total_ancestors()
        except AttributeError:
            return 0


class TestStack(TestCase):
    def test_simple(self):
        st = StackTest().execute()

    def test_stack_push_pop(self):
        p = plum.Process()
        with plum.stack.in_stack(p):
            self.assertIs(p, plum.stack.top())

        self.assertTrue(plum.stack.is_empty())
