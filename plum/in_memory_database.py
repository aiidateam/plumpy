
from plum.knowledge_provider import KnowledgeProvider, NotKnown
from plum.util import override, fullname
from plum.process_listener import ProcessListener
from plum.process_monitor import MONITOR, ProcessMonitorListener


class InMemoryDatabase(KnowledgeProvider, ProcessListener, ProcessMonitorListener):
    def __init__(self, retain_inputs, retain_outputs):
        self._retain_inputs = retain_inputs
        self._retain_outputs = retain_outputs
        self._inputs = {}
        self._outputs = {}
        self._pids_by_classname = {}
        self._finished = []
        # Listen for processes begin created and destroyed
        MONITOR.add_monitor_listener(self)

    @override
    def has_finished(self, pid):
        return pid in self._finished

    @override
    def get_output(self, pid, port_name):
        try:
            return self._outputs[pid][port_name]
        except KeyError:
            raise NotKnown()

    @override
    def get_outputs(self, pid):
        try:
            return self._outputs[pid]
        except KeyError:
            raise NotKnown()

    @override
    def get_input(self, pid, port_name):
        try:
            return self._inputs[pid][port_name]
        except KeyError:
            raise NotKnown()

    @override
    def get_inputs(self, pid):
        try:
            return self._inputs[pid]
        except KeyError:
            raise NotKnown()

    @override
    def get_pids_from_classname(self, classname):
        try:
            return self._pids_by_classname[classname]
        except KeyError:
            raise NotKnown()

    # ProcessMonitorListener events ############################################
    @override
    def on_monitored_process_created(self, process):
        process.add_process_listener(self)

        pids = self._pids_by_classname.setdefault(fullname(process), [])
        pids.append(process.pid)

        if self._retain_inputs:
            self._inputs[process.pid] = process.inputs
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
