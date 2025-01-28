# -*- coding: utf-8 -*-
import asyncio

import plumpy
from plumpy import BundleKeys

from . import utils
import pytest


class TestWaitingProcess:
    def test_instance_state(self):
        proc = utils.ThreeSteps()
        wl = utils.ProcessSaver(proc)
        proc.execute()

        for bundle, outputs in zip(wl.snapshots, wl.outputs):
            assert outputs == bundle.get(BundleKeys.OUTPUTS, {})

    def test_saving_each_step(self):
        loop = asyncio.get_event_loop()
        for proc_class in utils.TEST_WAITING_PROCESSES:
            proc = proc_class()
            saver = utils.ProcessSaver(proc)
            saver.capture()

            assert utils.check_process_against_snapshots(loop, proc_class, saver.snapshots)

    def test_kill(self):
        process = utils.WaitForSignalProcess()

        # Kill the process when it enters the WAITING state
        listener = plumpy.ProcessListener()
        listener.on_process_waiting = lambda _proc: process.kill()
        process.add_process_listener(listener)

        with pytest.raises(plumpy.KilledError):
            process.execute()
        assert process.killed()
