
from unittest import TestCase
from time import time

from plum.engine.parallel import MultithreadedEngine
from plum.test_utils import WaitForSignalProcess
from plum.process import ProcessState
from plum.util import override


class TestMultithreadedEngine(TestCase):
    @override
    def setUp(self):
        self.engine = MultithreadedEngine()

    def test_run(self):
        proc = WaitForSignalProcess.new_instance()
        fut = self.engine.run(proc)

        t0 = time()
        while time() - t0 < 10.:
            if proc.is_waiting():
                break
        self.assertEquals(proc.state, ProcessState.RUNNING)

        # Now it's waiting so signal that it can continue and wait for the
        # engine to make it happen
        proc.signal()
        t0 = time()
        while time() - t0 < 10.:
            if proc.state is ProcessState.DESTROYED:
                break
        self.assertEquals(proc.state, ProcessState.DESTROYED)

