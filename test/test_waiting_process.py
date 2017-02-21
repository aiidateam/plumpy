
import threading
from plum.persistence.bundle import Bundle
from plum.process import Process, ProcessState
from plum.process_monitor import MONITOR
from plum.test_utils import TwoCheckpoint, \
    DummyProcessWithOutput, TEST_WAITING_PROCESSES, WaitForSignalProcess
from plum.test_utils import ProcessListenerTester, check_process_against_snapshots
from plum.util import override
from plum.test_utils import ProcessSaver
from plum.wait_ons import wait_until_stopped, wait_until
from util import TestCase


class TestWaitingProcess(TestCase):
    def setUp(self):
        super(TestWaitingProcess, self).setUp()

        self.events_tester = ProcessListenerTester()
        self.proc = DummyProcessWithOutput.new()
        self.proc.add_process_listener(self.events_tester)

    def tearDown(self):
        super(TestWaitingProcess, self).tearDown()

        self.proc.remove_process_listener(self.events_tester)

    def test_instance_state(self):
        proc = TwoCheckpoint.new()
        wl = ProcessSaver(proc)
        proc.play()

        for snapshot, outputs in zip(wl.snapshots, wl.outputs):
            state, bundle = snapshot
            self.assertEqual(
                outputs, bundle[Process.BundleKeys.OUTPUTS.value].get_dict())

    def test_saving_each_step(self):
        for ProcClass in TEST_WAITING_PROCESSES:
            proc = ProcClass.new()
            saver = ProcessSaver(proc)
            try:
                proc.play()
            except BaseException:
                pass

            self.assertTrue(check_process_against_snapshots(ProcClass, saver.snapshots))

    def test_abort(self):
        p = WaitForSignalProcess.new()
        t = threading.Thread(target=p.play)

        # Start the process
        t.start()

        # Wait until it is waiting
        wait_until(p, ProcessState.WAITING)
        self.assertEqual(p.state, ProcessState.WAITING)

        # Abort it
        p.abort()

        # Wait until it's completely finished
        wait_until_stopped(p)
        self.assertEqual(p.state, ProcessState.STOPPED)
        self.assertTrue(p.has_aborted())

        self.safe_join(t)

    def _check_process_against_snapshot(self, snapshot, proc):
        self.assertEqual(snapshot.state, proc.state)

        new_bundle = Bundle()
        proc.save_instance_state(new_bundle)
        self.assertEqual(snapshot.bundle, new_bundle,
                         "Bundle mismatch with process class {}\n"
                         "Snapshot:\n{}\n"
                         "Loaded:\n{}".format(
                             proc.__class__, snapshot.bundle, new_bundle))

        self.assertEqual(snapshot.outputs, proc.outputs,
                         "Outputs mismatch with process class {}\n"
                         "Snapshot:\n{}\n"
                         "Loaded:\n{}".format(
                             proc.__class__, snapshot.outputs, proc.outputs))
