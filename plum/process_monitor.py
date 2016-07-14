
from abc import ABCMeta
from plum.process_listener import ProcessListener
from plum.util import EventHelper


class ProcessMonitorListener(object):
    __metaclass__ = ABCMeta

    def on_monitored_process_created(self, process):
        pass

    def on_monitored_process_destroying(self, process):
        pass

    def on_monitored_process_failed(self, pid):
        pass


class ProcessMonitor(ProcessListener):
    """
    This class is a central monitor that keeps track of all the currently
    running processes.

    Clients can listen for messages to indicate when a new process is registered
    and when processes terminate because of finishing or failing.
    """
    def __init__(self):
        self._processes = {}
        self.__event_helper = EventHelper(ProcessMonitorListener)

    def get_process(self, pid):
        return self._processes[pid]

    def get_pids(self):
        return self._processes.keys()

    def process_created(self, process):
        assert process.pid not in self._processes, \
               "A process with the same PID cannot be registered twice!"

        self._processes[process.pid] = process
        process.add_process_listener(self)
        self.__event_helper.fire_event(
            ProcessMonitorListener.on_monitored_process_created, process)

    def process_failed(self, pid):
        self.__event_helper.fire_event(
            ProcessMonitorListener.on_monitored_process_failed, pid)
        del self._processes[pid]

    def add_monitor_listener(self, listener):
        self.__event_helper.add_listener(listener)

    def remove_monitor_listener(self, listener):
        self.__event_helper.remove_listener(listener)

    # From ProcessListener #####################################################
    def on_process_destroy(self, process):
        process.remove_process_listener(self)
        del self._processes[process.pid]
        self.__event_helper.fire_event(
            ProcessMonitorListener.on_monitored_process_destroying, process)
    ############################################################################


monitor = ProcessMonitor()
