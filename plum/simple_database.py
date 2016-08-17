
from plum.process_database import ProcessDatabase
from plum.util import override
from plum.process_listener import ProcessListener
from plum.process_monitor import MONITOR, ProcessMonitorListener


class SimpleDatabase(ProcessDatabase, ProcessListener, ProcessMonitorListener):
    def __init__(self, retain_outputs):
        self._retain_outputs = retain_outputs
        if self._retain_outputs:
            self._outputs = {}
        self._finished = []
        # Listen for processes begin created and destroyed
        MONITOR.add_monitor_listener(self)

    @override
    def has_finished(self, pid):
        return pid in self._finished

    @override
    def get_output(self, pid, port):
        if self._retain_outputs:
            return self._outputs[pid][port]
        else:
            raise RuntimeError("Can only supply outputs if retain_ouputs is"
                               "specified on construction")

    @override
    def get_outputs(self, pid):
        if self._retain_outputs:
            return self._outputs[pid]
        else:
            raise RuntimeError("Can only supply outputs if retain_ouputs is"
                               "specified on construction")

    # ProcessMonitorListener events ############################################
    @override
    def on_monitored_process_created(self, process):
        process.add_process_listener(self)
    ############################################################################

    # Process messages ########################################################
    @override
    def on_output_emitted(self, process, output_port, value, dynamic):
        if self._retain_outputs:
            outputs = self._outputs.setdefault(process.pid, {})
            outputs[output_port] = value

    @override
    def on_process_finish(self, process):
        self._finished.append(process.pid)
    ###########################################################################