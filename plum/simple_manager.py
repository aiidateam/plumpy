

from plum.process_manager import ProcessManager
from plum.util import override
from plum.wait import WaitOn
import uuid


class SimpleManager(ProcessManager):
    def __init__(self, persistence=None):
        self._persistence = persistence
        self._my_processes = {}
        self._num_procs = 0

    @override
    def create_process(self, process_class, inputs=None, checkpoint=None):
        if checkpoint is not None:
            pid = checkpoint.pid
        else:
            pid = self._create_pid()

        proc = process_class()
        self._my_processes[pid] = proc
        if self._persistence is not None:
            self._persistence.persist_process(proc)

        proc_state = None
        wait_on = None

        # Check if we have a checkpoint and load the state if so
        if checkpoint is not None:
            proc_state = checkpoint.process_instance_state
            wait_on = WaitOn.create_from(checkpoint.wait_on_state, self)

        proc.on_create(pid, proc_state)
        return proc, wait_on

    @override
    def destroy_process(self, process):
        del self._my_processes[process.pid]
        process.on_destroy()

    @override
    def get_process(self, pid):
        return self._my_processes[pid]

    def _create_pid(self):
        return uuid.uuid1()


