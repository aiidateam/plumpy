
from plum.process_registry import ProcessRegistry
from plum.util import override
from plum.process import ProcessListener


class SimpleRegistry(ProcessRegistry, ProcessListener):
    def __init__(self, persistence_engine=None):
        self._running_processes = {}
        self._finished = {}
        self._persistence_engine = persistence_engine

    @override
    def register_running_process(self, process):
        self._running_processes[process.pid] = process
        process.add_process_listener(self)
        if self._persistence_engine:
            self._persistence_engine.persist_process(process)

    @override
    def get_running_process(self, pid):
        return self._running_processes[pid]

    @override
    def is_finished(self, pid):
        return pid in self._finished

    @override
    def get_outputs(self, pid):
        return self._finished[pid]

    # Process messages
    @override
    def on_process_finish(self, process, retval):
        process.remove_process_listener(self)
        del self._running_processes[process.pid]
        self._finished[process.pid] = process.get_last_outputs()

