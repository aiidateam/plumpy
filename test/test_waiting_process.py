import plumpy
from plumpy import Process, ProcessState, BundleKeys
from plumpy.test_utils import ThreeSteps, \
    DummyProcessWithOutput, TEST_WAITING_PROCESSES, WaitForSignalProcess
from plumpy.test_utils import check_process_against_snapshots
from plumpy.test_utils import ProcessSaver

from . import utils


class TestWaitingProcess(utils.TestCaseWithLoop):
    def test_instance_state(self):
        proc = ThreeSteps()
        wl = ProcessSaver(proc)
        proc.execute()

        for bundle, outputs in zip(wl.snapshots, wl.outputs):
            self.assertEqual(outputs, bundle.get(BundleKeys.OUTPUTS, {}))

    def test_saving_each_step(self):
        for proc_class in TEST_WAITING_PROCESSES:
            proc = proc_class()
            saver = ProcessSaver(proc)
            saver.capture()

            self.assertTrue(check_process_against_snapshots(self.loop, proc_class, saver.snapshots))

    def test_abort(self):
        process = WaitForSignalProcess()
        process.add_on_waiting_callback(lambda _: process.pause())
        process.execute()
        process.kill()
        self.assertTrue(process.killed())

    def _check_process_against_snapshot(self, snapshot, proc):
        self.assertEqual(snapshot.state, proc.state)

        new_bundle = plumpy.Bundle()
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
