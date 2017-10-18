import apricotpy
import plum
from plum import loop_factory
from plum import Process, ProcessState
from plum.test_utils import TwoCheckpoint, \
    DummyProcessWithOutput, TEST_WAITING_PROCESSES, WaitForSignalProcess
from plum.test_utils import check_process_against_snapshots
from plum.utils import override
from plum.test_utils import ProcessSaver
from plum.wait_ons import run_until
from util import TestCase


class TestWaitingProcess(TestCase):
    def setUp(self):
        super(TestWaitingProcess, self).setUp()

        self.loop = loop_factory()

    def test_instance_state(self):
        proc = ~self.loop.create_inserted(TwoCheckpoint)
        wl = ProcessSaver(proc)
        self.loop.run_until_complete(proc)

        for bundle, outputs in zip(wl.snapshots, wl.outputs):
            self.assertEqual(outputs, bundle[plum.process.BundleKeys.OUTPUTS])

    def test_saving_each_step(self):
        for proc_class in TEST_WAITING_PROCESSES:
            proc = self.loop.create(proc_class)
            saver = ProcessSaver(proc)
            try:
                self.loop.run_until_complete(proc)
            except BaseException:
                pass

            self.assertTrue(check_process_against_snapshots(self.loop, proc_class, saver.snapshots))

    def test_abort(self):
        p = self.loop.create(WaitForSignalProcess)

        # Wait until it is waiting
        run_until(p, ProcessState.WAITING, self.loop)

        # Abort it
        p.abort()

        # Wait until it's completely finished
        run_until(p, ProcessState.STOPPED, self.loop)
        self.assertTrue(p.has_aborted())

    def _check_process_against_snapshot(self, snapshot, proc):
        self.assertEqual(snapshot.state, proc.state)

        new_bundle = apricotpy.Bundle()
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
