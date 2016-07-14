
from plum.process_registry import ProcessRegistry
from plum.util import override
from plum.process_listener import ProcessListener
from plum.process_monitor import monitor, ProcessMonitorListener


class SimpleRegistry(ProcessRegistry, ProcessListener, ProcessMonitorListener):
    def __init__(self):
        self._running_processes = {}
        self._finished = {}
        # Listen for processes begin created and destroyed
        monitor.add_monitor_listener(self)

    @override
    def get_running_process(self, pid):
        return monitor.get_process(pid)

    @override
    def is_finished(self, pid):
        try:
            self.get_running_process(pid)
            return False
        except KeyError:
            pass

        # Maybe it's finished
        if pid in self._finished:
            return True
        else:
            raise ValueError("Unknown process pid '{}'".format(pid))

    @override
    def get_output(self, pid, port):
        try:
            return self._finished[pid][port]
        except KeyError:
            raise ValueError("Process not finished.")

    @override
    def get_outputs(self, pid):
        return self._finished[pid]

    # Process messages
    @override
    def on_process_finish(self, process, retval):
        self._finished[process.pid] = process.get_last_outputs()

    # ProcessMonitorListener events ############################################
    def on_monitored_process_created(self, process):
        process.add_process_listener(self)
    ############################################################################
