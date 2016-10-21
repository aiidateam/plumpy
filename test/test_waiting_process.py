from unittest import TestCase

import threading
from plum.persistence.bundle import Bundle
from plum.process import Process, ProcessState
from plum.process_monitor import MONITOR
from plum.test_utils import TwoCheckpointProcess, \
    DummyProcessWithOutput, TEST_WAITING_PROCESSES, WaitForSignalProcess
from plum.test_utils import ProcessListenerTester, check_process_against_snapshots
from plum.util import override
from plum.test_utils import WaitSaver, ProcessWaitSaver
from plum.wait_ons import wait_until_destroyed, wait_until_state


class TestWaitingProcess(TestCase):
    def setUp(self):
        self.events_tester = ProcessListenerTester()
        self.proc = DummyProcessWithOutput()
        self.proc.add_process_listener(self.events_tester)

    def tearDown(self):
        self.proc.remove_process_listener(self.events_tester)

    def test_instance_state(self):
        proc = TwoCheckpointProcess.new_instance()
        wl = WaitSaver(proc)
        proc.start()

        for snapshot, outputs in zip(wl.snapshots, wl.outputs):
            state, bundle = snapshot
            self.assertEqual(
                outputs, bundle[Process.BundleKeys.OUTPUTS.value].get_dict())

    def test_saving_each_step(self):
        for ProcClass in TEST_WAITING_PROCESSES:
            proc = ProcClass.new_instance()
            saver = ProcessWaitSaver(proc)
            try:
                proc.start()
            except BaseException:
                pass

            self.assertTrue(check_process_against_snapshots(ProcClass, saver.snapshots))

    def test_abort(self):
        p = WaitForSignalProcess.new_instance()
        t = threading.Thread(target=p.start)

        # Start the process
        t.start()

        # Wait until it is running
        wait_until_state(p, ProcessState.RUNNING)
        self.assertEqual(p.state, ProcessState.RUNNING)

        # Abort it
        self.assertTrue(p.abort())

        # Wait until it's completely finished
        wait_until_destroyed(p)
        self.assertEqual(p.state, ProcessState.DESTROYED)
        self.assertTrue(p.aborted)

        t.join()

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
