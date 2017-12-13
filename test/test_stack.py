from test.util import TestCase
import plum
import plum.stack as stack


class StackTest(plum.Process):
    def _run(self):
        assert len(stack.stack()) == 1
        assert stack.top() is self

    def total_ancestors(self):
        try:
            return 1 + self.inputs.parent.total_ancestors()
        except AttributeError:
            return 0


class TestStack(TestCase):
    def test_simple(self):
        StackTest().execute()
