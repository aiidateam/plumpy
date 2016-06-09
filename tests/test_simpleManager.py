from unittest import TestCase
from plum.simple_manager import SimpleManager
from plum.util import override
from tests.common import ProcessEventsTester


class TestSimpleManager(TestCase):
    @override
    def setUp(self):
        self.simple_manager = SimpleManager()

    def test_create_process(self):
        proc = self.simple_manager.create_process(ProcessEventsTester)[0]
        self.assertTrue(isinstance(proc, ProcessEventsTester))

        self.assertTrue(proc.get_last_outputs().get('create', False))

    def test_destroy_process(self):
        proc = self.simple_manager.create_process(ProcessEventsTester)[0]
        self.simple_manager.destroy_process(proc)
        self.assertTrue(proc.get_last_outputs().get('destroy', False))

    def test_get_process(self):
        proc = self.simple_manager.create_process(ProcessEventsTester)[0]
        self.assertIs(proc, self.simple_manager.get_process(proc.pid))
