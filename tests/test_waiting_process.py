# -*- coding: utf-8 -*-
import asyncio
import unittest

import plumpy
from plumpy import BundleKeys

from . import utils


class TestWaitingProcess(unittest.TestCase):
    def test_instance_state(self):
        proc = utils.ThreeSteps()
        wl = utils.ProcessSaver(proc)
        proc.execute()

        for bundle, outputs in zip(wl.snapshots, wl.outputs):
            self.assertEqual(outputs, bundle.get(BundleKeys.OUTPUTS, {}))

    def test_saving_each_step(self):
        loop = asyncio.get_event_loop()
        for proc_class in utils.TEST_WAITING_PROCESSES:
            proc = proc_class()
            saver = utils.ProcessSaver(proc)
            saver.capture()

            self.assertTrue(utils.check_process_against_snapshots(loop, proc_class, saver.snapshots))

    def test_kill(self):
        process = utils.WaitForSignalProcess()

        # Kill the process when it enters the WAITING state
        listener = plumpy.ProcessListener()
        listener.on_process_waiting = lambda _proc: process.kill()
        process.add_process_listener(listener)

        with self.assertRaises(plumpy.KilledError):
            process.execute()
        self.assertTrue(process.killed())
