from unittest import TestCase
from plum.simple_factory import SimpleFactory
from plum.util import override
from tests.common import ProcessEventsTester


class TestSimpleManager(TestCase):
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
