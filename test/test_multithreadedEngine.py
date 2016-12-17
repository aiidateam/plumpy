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
        self.assertEqual(len(MONITOR.get_pids()), 0)
        self.engine = MultithreadedEngine()

    @override
    def tearDown(self):
        self.assertEqual(len(MONITOR.get_pids()), 0)

    def test_run(self):
        proc = WaitForSignalProcess.new_instance()
        fut = self.engine.start(proc)

        t0 = time()
        while time() - t0 < 10.:
            if proc.state is ProcessState.WAITING:
                break

        # Now it's waiting so signal that it can continue and wait for the
        # engine to make it happen
        proc.signal()
        t0 = time()
        while time() - t0 < 10.:
            if proc.has_terminated():
                break
        self.assertEquals(proc.state, ProcessState.STOPPED)

    def test_submit_simple(self):
        self.engine.submit(common.ProcessEventsTester).result()
        self._test_engine_events(
            common.ProcessEventsTester.called_events,
            ['create', 'wait', 'resume', 'exception'])


    def test_submit_with_checkpoint(self):
        self.engine.submit(common.TwoCheckpoint).result()
        self._test_engine_events(
            common.TwoCheckpoint.called_events,
            ['create', 'exception'])


    def test_submit_exception(self):
        e = self.engine.submit(common.ExceptionProcess).exception()
        self.assertIsInstance(e, BaseException)
        self._test_engine_events(
            common.ExceptionProcess.called_events,
            ['finish', 'wait', 'resume', 'stop'])


    def test_submit_checkpoint_then_exception(self):
        e = self.engine.submit(common.TwoCheckpointThenException). \
            exception()
        self.assertIsInstance(e, RuntimeError)
        self._test_engine_events(
            common.TwoCheckpointThenException.called_events,
            ['stop', 'finish'])

    def test_exception_monitor_notification(self):
        """
        This test checks that when a process fails the engine notifies the
        process monitor of the failure.
        """

        class MonitorListener(ProcessMonitorListener):
            def __init__(self):
                MONITOR.add_monitor_listener(self)
                self.created_called = False
                self.failed_called = False

            @override
            def on_monitored_process_registered(self, process):
                self.created_called = True

            @override
            def on_monitored_process_failed(self, pid):
                self.failed_called = True

        l = MonitorListener()
        self.engine.submit(common.TwoCheckpointThenException).exception()
        self.assertTrue(l.created_called)
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
        f.result()

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
