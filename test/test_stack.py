from test.util import TestCase
import plum
import plum.stack as stack
from . import util


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
        loop = plum.new_event_loop()
        stack_test = StackTest(loop=loop)
        stack_test.play()
        stack_test.execute()
