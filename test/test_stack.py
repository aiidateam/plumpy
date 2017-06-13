from test.util import TestCase
from plum import Process
import plum.stack as stack


class StackTest(Process):
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
        StackTest.launch()

