from abc import ABCMeta
from plum.process_listener import ProcessListener
from plum.util import EventHelper, ListenContext, override


class ProcessMonitorListener(object):
    """
    The interface for a process monitor listener.  Override any of the methods
    to receive these messages.
    """
    __metaclass__ = ABCMeta

    def on_monitored_process_registered(self, process):
        pass

    def on_monitored_process_finish(self, process):
        pass


class ProcessMonitor(ProcessListener):
    """
    This class is a central monitor that keeps track of all the currently
    playing processes.  Think of it as the process manager in your OS that shows
    you what is currently running.

    Clients can listen for messages to indicate when a new process is registered
    and when processes terminate because of finishing or failing.
    """

    def __init__(self):
        self._processes = {}
        self.__event_helper = EventHelper(ProcessMonitorListener)

    def get_process(self, pid):
        """
        Get the process instance for a currently running process.

        :param pid: The pid of the process instance to get.
        :return: The process with the given pid.
        :raises: ValueError
        """
        try:
            return self._processes[pid]
        except KeyError:
            raise ValueError("Unknown pid '{}'".format(pid))

    def get_processes(self):
        return self._processes.values()

    def get_pids(self):
        """
        Get the pids of all currently running processes.

        :return: A sequence of pids.
        """
        return self._processes.keys()

    def register_process(self, process):
        """
        Called by the :class:`~plum.process.Process` to inform the monitor
        about this process.

        :param process: The process that is being registered
        :type process: :class:`~plum.process.Process`
        """
        assert process.pid not in self._processes, \
            "A process with the same PID cannot be registered twice!"

        self._processes[process.pid] = process
        process.add_process_listener(self)
        self.__event_helper.fire_event(
            ProcessMonitorListener.on_monitored_process_registered, process)

    def deregister_process(self, process):
        process.remove_process_listener(self)
        del self._processes[process.pid]

    def listen(self, listener):
        return ListenContext(self, listener)

    def start_listening(self, listener):
        self.__event_helper.add_listener(listener)

    def stop_listening(self, listener):
        self.__event_helper.remove_listener(listener)

    def get_num_listeners(self):
        return len(self.__event_helper.listeners)

    # region From ProcessListener
    @override
    def on_process_finish(self, process):
        self.__event_helper.fire_event(
            ProcessMonitorListener.on_monitored_process_finish, process)

    # endregion


# The global singleton
MONITOR = ProcessMonitor()
