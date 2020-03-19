# -*- coding: utf-8 -*-
"""Savable Process tests"""
import asyncio
import pytest

import plumpy
from plumpy import BundleKeys, ProcessState

from .. import utils


@plumpy.auto_persist('steps_ran')
class SavePauseProc(plumpy.Process):
    steps_ran = None

    def init(self):
        super(SavePauseProc, self).init()
        self.steps_ran = []

    def run(self):
        self.pause()
        self.steps_ran.append(self.run.__name__)
        return plumpy.Continue(self.step2)

    def step2(self):
        self.steps_ran.append(self.step2.__name__)


def _check_round_trip(proc1):
    bundle1 = plumpy.Bundle(proc1)

    proc2 = bundle1.unbundle()
    bundle2 = plumpy.Bundle(proc2)

    assert proc1.pid == proc2.pid
    assert bundle1 == bundle2


@pytest.mark.asyncio
async def test_running_save_instance_state():  # pylint: disable=invalid-name
    nsync_comeback = SavePauseProc()
    asyncio.ensure_future(nsync_comeback.step_until_terminated())

    await utils.run_until_paused(nsync_comeback)

    # Create a checkpoint
    bundle = plumpy.Bundle(nsync_comeback)
    assert [SavePauseProc.run.__name__] == nsync_comeback.steps_ran

    nsync_comeback.play()
    await nsync_comeback.future()

    assert [SavePauseProc.run.__name__, SavePauseProc.step2.__name__] == nsync_comeback.steps_ran

    proc_unbundled = bundle.unbundle()

    # At bundle time the Process was paused, the future of which will be persisted to the bundle.
    # As a result the process, recreated from that bundle, will also be paused and will have to be played
    proc_unbundled.play()
    assert not proc_unbundled.steps_ran
    await proc_unbundled.step_until_terminated()
    assert [SavePauseProc.step2.__name__] == proc_unbundled.steps_ran


def test_created_bundle():
    """
    Check that the bundle after just creating a process is as we expect
    """
    _check_round_trip(utils.DummyProcess())


def test_instance_state_with_outputs():  # pylint: disable=invalid-name
    proc = utils.DummyProcessWithOutput()

    saver = utils.ProcessSaver(proc)
    proc.execute()

    _check_round_trip(proc)

    for bundle, outputs in zip(saver.snapshots, saver.outputs):
        # Check that it is a copy
        assert outputs is not bundle.get(BundleKeys.OUTPUTS, {})
        # Check the contents are the same
        assert outputs == bundle.get(BundleKeys.OUTPUTS, {})

    assert proc.outputs is not saver.snapshots[-1].get(BundleKeys.OUTPUTS, {})


def test_saving_each_step():
    loop = asyncio.get_event_loop()
    for proc_class in utils.TEST_PROCESSES:
        proc = proc_class()
        saver = utils.ProcessSaver(proc)
        saver.capture()
        assert proc.state == ProcessState.FINISHED
        assert utils.check_process_against_snapshots(loop, proc_class, saver.snapshots)


@pytest.mark.asyncio
async def test_restart():
    proc = _RestartProcess()
    asyncio.ensure_future(proc.step_until_terminated())

    await utils.run_until_waiting(proc)

    # Save the state of the process
    saved_state = plumpy.Bundle(proc)

    # Load a process from the saved state
    loaded_proc = saved_state.unbundle()
    assert loaded_proc.state == ProcessState.WAITING

    # Now resume it
    loaded_proc.resume()
    await loaded_proc.step_until_terminated()
    assert loaded_proc.outputs == {'finished': True}


@pytest.mark.asyncio
async def test_wait_save_continue():
    """ Test that process saved while in WAITING state restarts correctly when loaded """
    proc = utils.WaitForSignalProcess()
    asyncio.ensure_future(proc.step_until_terminated())

    await utils.run_until_waiting(proc)

    saved_state = plumpy.Bundle(proc)

    # Run the process to the end
    proc.resume()
    result1 = await proc.future()

    # Load from saved state and run again
    loader = plumpy.get_object_loader()
    proc2 = saved_state.unbundle(plumpy.LoadSaveContext(loader))
    asyncio.ensure_future(proc2.step_until_terminated())
    proc2.resume()
    result2 = await proc2.future()

    # Check results match
    assert result1 == result2


def test_killed():
    proc = utils.DummyProcess()
    proc.kill()
    assert proc.state == ProcessState.KILLED
    _check_round_trip(proc)


class _RestartProcess(utils.WaitForSignalProcess):

    @classmethod
    def define(cls, spec):
        super(_RestartProcess, cls).define(spec)
        spec.outputs.dynamic = True

    def last_step(self):
        self.out('finished', True)
