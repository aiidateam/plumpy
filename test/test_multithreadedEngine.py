from time import time
from unittest import TestCase

from plum.parallel import MultithreadedEngine
from plum.process import ProcessState
from plum.test_utils import WaitForSignalProcess
from plum.util import override
import plum.test_utils as common
from plum.process_monitor import ProcessMonitorListener, MONITOR


class TestMultithreadedEngine(TestCase):
    @override
    def setUp(self):
        self.engine = MultithreadedEngine()

    def test_run(self):
        proc = WaitForSignalProcess.new_instance()
        fut = self.engine.start(proc)

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

    def test_submit_simple(self):
        self.engine.submit(common.ProcessEventsTester).result()
        self._test_engine_events(
            common.ProcessEventsTester.called_events,
            ['create', 'wait', 'continue', 'exception'])


    def test_submit_with_checkpoint(self):
        self.engine.submit(common.TwoCheckpointProcess).result()
        self._test_engine_events(
            common.TwoCheckpointProcess.called_events,
            ['create', 'exception'])


    def test_submit_exception(self):
        e = self.engine.submit(common.ExceptionProcess).exception()
        self.assertIsInstance(e, BaseException)
        self._test_engine_events(
            common.ExceptionProcess.called_events,
            ['finish', 'wait', 'continue', 'stop', 'destroy'])


    def test_submit_checkpoint_then_exception(self):
        e = self.engine.submit(common.TwoCheckpointThenExceptionProcess). \
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

        l = MonitorListener()
        self.engine.submit(common.TwoCheckpointThenExceptionProcess).exception()
        self.assertTrue(l.registered_called)
        self.assertTrue(l.failed_called)


    def test_future_ready(self):
        f = self.engine.submit(common.DummyProcess)
        f.result()
        self.assertTrue(f.done())


    # Not sure how to do this test because normally I expect the
    # KeyboardInterrupt exception on the main thread...
    # but this could lead to one one a different thread...
    #
    def test_keyboard_interrupt(self):
        # Make sure the serial engine raises this error
        with self.assertRaises(KeyboardInterrupt):
            self.engine.submit(common.KeyboardInterruptProc).result()

    def test_futures(self):
        dp = common.DummyProcess.new_instance()
        f = self.engine.start(dp)
        self.assertEqual(f.pid, dp.pid)
        f.process.abort()

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
                              "Event '{}' not called by self.engine.".format(event))
