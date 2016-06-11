from unittest import TestCase

from plum.engine.parallel import MultithreadedEngine
from plum.engine.serial import SerialEngine
from plum.engine.ticking import TickingEngine
from concurrent.futures import ThreadPoolExecutor
import threading
import tests.common as common


class TestExecutionEngine(TestCase):
    def setUp(self):
        self.ticking_engine = TickingEngine()
        self.event = threading.Event()
        self.pool = ThreadPoolExecutor(1)
        self.fut = self.pool.submit(self.tick_ticking)

        self.engines_to_test = [
            SerialEngine(),
            #self.ticking_engine
            #MultithreadedEngine()
        ]

    def tearDown(self):
        self.event.set()
        #self.fut.result()
        pass

    def test_submit_simple(self):
        for engine in self.engines_to_test:
            engine.submit(common.ProcessEventsTester, None).result()
            self._test_engine_events(
                common.ProcessEventsTester.called_events,
                ['recreate', 'wait', 'continue', 'exception'])

    def test_submit_with_checkpoint(self):
        for engine in self.engines_to_test:
            engine.submit(common.CheckpointProcess, None).result()
            self._test_engine_events(common.CheckpointProcess.called_events,
                                     ['recreate', 'exception'])

    def test_submit_exception(self):
        """
        The raising of an exception by the process should still lead to an
        on_stop and on_destroy message
        """
        for engine in self.engines_to_test:
            e = engine.submit(common.ExceptionProcess, None).exception()
            self.assertIsInstance(e, RuntimeError)
            self._test_engine_events(common.ExceptionProcess.called_events,
                                     ['recreate', 'finish', 'wait', 'continue'])

    def test_submit_checkpoint_then_exception(self):
        """
        The raising of an exception by the process should still lead to an
        on_stop and on_destroy message
        """
        for engine in self.engines_to_test:
            e = engine.submit(common.CheckpointThenExceptionProcess, None).\
                exception()
            self.assertIsInstance(e, RuntimeError)
            self._test_engine_events(
                common.CheckpointThenExceptionProcess.called_events,
                ['recreate', 'finish'])

    def tick_ticking(self):
        import time
        while not self.event.is_set():
            self.ticking_engine.tick()
            time.sleep(2)

    def _test_engine_events(self, outs, exclude_events):
        """
        Check that all the events have been called except those that are
        specifically excluded.
        :param outs: The outputs of the process.
        :param exclude_events: The events not to check for
        """
        for event in common.ProcessEventsTester.EVENTS:
            if event not in exclude_events:
                self.assertIn(event, outs,
                              "Event {} not called by engine.".format(event))
