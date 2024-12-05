# -*- coding: utf-8 -*-
import pytest
from tests import utils

import plumpy
from plumpy import process_comms


class Process(plumpy.Process):
    def run(self):
        pass


class CustomObjectLoader(plumpy.DefaultObjectLoader):
    def load_object(self, identifier):
        if identifier == 'jimmy':
            return Process
        else:
            return super().load_object(identifier)

    def identify_object(self, obj):
        if isinstance(obj, Process) or issubclass(obj, Process):
            return 'jimmy'
        else:
            return super().identify_object(obj)


@pytest.mark.asyncio
async def test_continue():
    persister = plumpy.InMemoryPersister()
    load_context = plumpy.LoadSaveContext()
    launcher = plumpy.ProcessLauncher(persister=persister, load_context=load_context)

    process = utils.DummyProcess()
    pid = process.pid
    persister.save_checkpoint(process)
    del process
    process = None

    result = await launcher._continue(None, **plumpy.create_continue_body(pid)[process_comms.TASK_ARGS])
    assert result == utils.DummyProcess.EXPECTED_OUTPUTS


@pytest.mark.asyncio
async def test_loader_is_used():
    """Make sure that the provided class loader is used by the process launcher"""
    loader = CustomObjectLoader()
    proc = Process()
    persister = plumpy.InMemoryPersister(loader=loader)
    persister.save_checkpoint(proc)
    launcher = plumpy.ProcessLauncher(persister=persister, loader=loader)

    continue_task = plumpy.create_continue_body(proc.pid)
    result = await launcher._continue(None, **continue_task[process_comms.TASK_ARGS])
    assert result == utils.DummyProcess.EXPECTED_OUTPUTS
