
from abc import ABCMeta
from plum.process_listener import ProcessListener
from plum.util import EventHelper, override


class ProcessMonitorListener(object):
    """
    The interface for a process monitor listener.  Override any of the methods
    to receive these messages.
    """
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
    running processes.  Think of it as the process manager in your OS that shows
    you what is currently running.

    Clients can listen for messages to indicate when a new process is registered
    and when processes terminate because of finishing or failing.
    """
    def __init__(self):
        self._processes = {}
        self.__event_helper = EventHelper(ProcessMonitorListener)

    def get_process(self, pid):
        try:
            return self._processes[pid]
        except KeyError:
            raise ValueError("Unknown pid '{}'".format(pid))

    def get_pids(self):
        return self._processes.keys()

    def reset(self):
        """
        Reset the monitor by stopping listening for messages from any existing
        Processes.  Be very careful with this as some of the clients may be
        expecting to get messages about what is happening which will not be
        sent after this call.
        """
        for proc in self._processes.itervalues():
            proc.remove_process_listener(self)
        self._processes = {}

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
    @override
    def on_process_destroy(self, process):
        self.__event_helper.fire_event(
            ProcessMonitorListener.on_monitored_process_destroying, process)

        process.remove_process_listener(self)
        del self._processes[process.pid]
    ############################################################################


# The global singleton
MONITOR = ProcessMonitor()
