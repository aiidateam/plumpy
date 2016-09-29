from unittest import TestCase
import thread
import threading

import plum.test_utils as common
from plum.engine.serial import SerialEngine
from plum.engine.parallel import MultithreadedEngine
from plum.engine.ticking import TickingEngine
from plum.util import override
from plum.process_monitor import MONITOR, ProcessMonitorListener


class TestExecutionEngine(TestCase):
    """
    General tests that all execution engines are expected to be able to pass.
    """

    @override
    def setUp(self):
        from threading import Thread

        ticking = TickingEngine()
        self.stop_ticking = threading.Event()
        self.thread = Thread(target=self.tick_ticking, args=(ticking,))
        self.thread.start()

        serial = SerialEngine()
        self.engines_to_test = [
            serial,
            ticking,
            MultithreadedEngine()
        ]

    @override
    def tearDown(self):
        self.stop_ticking.set()
        self.thread.join()

    def test_submit_simple(self):
        for engine in self.engines_to_test:
            engine.submit(common.ProcessEventsTester, None).result()
            self._test_engine_events(
                common.ProcessEventsTester.called_events,
                ['create', 'wait', 'continue', 'exception'])

    def test_submit_with_checkpoint(self):
        for engine in self.engines_to_test:
            engine.submit(common.TwoCheckpointProcess, None).result()
            self._test_engine_events(
                common.TwoCheckpointProcess.called_events,
                ['create', 'exception'])

    def test_submit_exception(self):
        for engine in self.engines_to_test:
            e = engine.submit(common.ExceptionProcess, None).exception()
            self.assertIsInstance(e, BaseException)
            self._test_engine_events(
                common.ExceptionProcess.called_events,
                ['finish', 'wait', 'continue', 'stop', 'finish', 'destroy'])

    def test_submit_checkpoint_then_exception(self):
        for engine in self.engines_to_test:
            e = engine.submit(common.TwoCheckpointThenExceptionProcess, None).\
                exception()
            self.assertIsInstance(e, RuntimeError)
            self._test_engine_events(
                common.TwoCheckpointThenExceptionProcess.called_events,
                ['stop', 'finish', 'destroy'])

    def test_exception_monitor_notification(self):
        """
        This test checks that when a process fails the engine notifies the
        process monitor of the failure.
        """
        class MonitorListener(ProcessMonitorListener):
            def __init__(self):
                MONITOR.add_monitor_listener(self)
                self.registered_called = False
                self.failed_called = False

            @override
            def on_monitored_process_created(self, process):
                self.registered_called = True

            @override
            def on_monitored_process_failed(self, pid):
                self.failed_called = True

        for engine in self.engines_to_test:
            l = MonitorListener()
            engine.submit(common.TwoCheckpointThenExceptionProcess).exception()
            self.assertTrue(l.registered_called)
            self.assertTrue(l.failed_called)

    def tick_ticking(self, engine):
        """
        Keep ticking the ticking engine until the event is set to stop
        :param engine: the ticking engine
        """
        while not self.stop_ticking.is_set():
            try:
                engine.tick()
            except KeyboardInterrupt:
                thread.interrupt_main()

    def test_future_ready(self):
        for engine in self.engines_to_test:
            f = engine.submit(common.DummyProcess, None)
            f.result()
            self.assertTrue(f.done())

    # Not sure how to do this test because normally I expect the
    # KeyboardInterrupt exception on the main thread...
    # but this could lead to one one a different thread...
    #
    def test_keyboard_interrupt(self):
        for e in self.engines_to_test:
            # Make sure the serial engine raises this error
            with self.assertRaises(KeyboardInterrupt):
                e.submit(common.KeyboardInterruptProc).result()

    def test_futures(self):
        for engine in self.engines_to_test:
            dp = common.DummyProcess.new_instance()
            f = engine.run(dp)
            self.assertEqual(f.pid, dp.pid)

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
                              "Event '{}' not called by engine.".format(event))
