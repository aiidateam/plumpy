from unittest import TestCase

from plum.test_utils import ProcessEventsTester
from plum.simple_factory import SimpleFactory
from plum.util import override


class TestSimpleFactory(TestCase):
    @override
    def setUp(self):
        self.simple_manager = SimpleFactory()

    def test_create_process(self):
        proc = self.simple_manager.create_process(ProcessEventsTester)
        self.assertTrue(isinstance(proc, ProcessEventsTester))

        self.assertIn("create", ProcessEventsTester.called_events)

    def test_create_checpoint(self):
        # TODO
        pass
