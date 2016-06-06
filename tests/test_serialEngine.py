from unittest import TestCase
from plum.serial_engine import SerialEngine
from plum.process import Process
from plum.util import override


class DummyProcess(Process):
    @override
    def _run(self):
        pass


class TestSerialEngine(TestCase):
    def test_submit(self):
        pass
