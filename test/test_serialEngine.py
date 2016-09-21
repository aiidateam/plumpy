from unittest import TestCase

from plum.engine.serial import SerialEngine
from plum.process import Process
from plum.util import override


class TestSerialEngine(TestCase):
    def setUp(self):
        self.engine = SerialEngine()
