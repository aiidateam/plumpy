from unittest import TestCase

from plum.engine.serial import SerialEngine
from plum.process import Process
from plum.util import override


class KeyboardInterruptProc(Process):
    @override
    def _run(self):
        raise KeyboardInterrupt()


class TestSerialEngine(TestCase):
    def setUp(self):
        self.engine = SerialEngine()

    def test_keyboard_interrupt(self):
        # Make sure the serial engine raises this error
        with self.assertRaises(KeyboardInterrupt):
            self.engine.submit(KeyboardInterruptProc, None)
