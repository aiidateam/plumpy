
from unittest import TestCase
from plum.serial_engine import SerialEngine
from plum.parallel import MultithreadedEngine
from tests.common import ProcessEventsTester


class TestExecutionEngine(TestCase):
    def setUp(self):
        self.engines_to_test = [
            SerialEngine(),
            MultithreadedEngine()
        ]
        self.test_process = ProcessEventsTester()

    def test_submit(self):
        for engine in self.engines_to_test:
            outs = engine.submit(ProcessEventsTester, None).result()
            for event in ProcessEventsTester.EVENTS:
                if event not in ['stop', 'destroy']:
                    self.assertTrue(
                        outs.get(event, False),
                        "Engine {} did not call event {}.".format(engine, event)
                    )
