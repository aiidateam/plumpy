

from plum.process_factory import ProcessFactory
from plum.util import override
from plum.wait import WaitOn
from plum.persistence.checkpoint import Checkpoint
from plum.process import Process
import uuid


class SimpleFactory(ProcessFactory):
    def __init__(self):
        self._num_procs = 0

    @override
    def create_process(self, process_class, inputs=None):
        proc = process_class()
        proc.perform_create(self._create_pid(), inputs)
        return proc

    @override
    def recreate_process(self, checkpoint):
        pid = checkpoint.pid

        proc = checkpoint.process_class()

        wait_on = WaitOn.create_from(checkpoint.wait_on_state, self)
        inputs = checkpoint.process_instance_state[Process._INPUTS]

        proc.perform_create(pid, inputs, checkpoint.process_instance_state)
        return proc, wait_on

    @override
    def create_checkpoint(self, process, wait_on=None):
        cp = Checkpoint()
        cp.pid = process.pid
        cp.process_class = process.__class__
        process.save_instance_state(cp.process_instance_state)
        if wait_on is not None:
            wait_on.save_instance_state(cp.wait_on_instance_state)

        return cp

    def _create_pid(self):
        return uuid.uuid1()


