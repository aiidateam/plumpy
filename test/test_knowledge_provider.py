from unittest import TestCase

from plum.knowledge_provider import NotKnown
from plum.knowledge_base import KnowledgeBase
from plum.in_memory_database import InMemoryDatabase


class TestKnowledgeProviders(TestCase):
    def setUp(self):
        self.providers = [KnowledgeBase(), InMemoryDatabase(True, True)]

    def test_unknown_processes(self):
        """
        Check that the providers raise NotKnown if they are asked questions
        about a non-existent process.
        """
        pid = "NOT KNOWN"
        for p in self.providers:
            with self.assertRaises(NotKnown):
                p.has_finished(pid)
            with self.assertRaises(NotKnown):
                p.get_outputs(pid)
            with self.assertRaises(NotKnown):
                p.get_output(pid, "FAKE PORT")
            with self.assertRaises(NotKnown):
                p.get_inputs(pid)
            with self.assertRaises(NotKnown):
                p.get_input(pid, "FAKE PORT")
            with self.assertRaises(NotKnown):
                p.get_pids_from_classname("made.up.class")