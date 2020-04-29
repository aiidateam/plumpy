# -*- coding: utf-8 -*-
import plumpy
from plumpy import Process, ProcessState, BundleKeys
from test.test_utils import ThreeSteps, \
    DummyProcessWithOutput, TEST_WAITING_PROCESSES, WaitForSignalProcess, \
    check_process_against_snapshots, ProcessSaver

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

    def test_kill(self):
        process = WaitForSignalProcess()

        # Kill the process when it enters the WAITING state
        listener = plumpy.ProcessListener()
        listener.on_process_waiting = lambda _proc: process.kill()
        process.add_process_listener(listener)

        with self.assertRaises(plumpy.KilledError):
            process.execute()
        self.assertTrue(process.killed())
