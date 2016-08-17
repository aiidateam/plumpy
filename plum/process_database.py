
from abc import ABCMeta, abstractmethod
from plum.process_monitor import MONITOR


class ProcessDatabase(object):
    __metaclass__ = ABCMeta

    def get_active_process(self, pid):
        """
        Get an active process.  This is a convenience method that calls through
        to the process monitor and is quivalent to monitor.get_process(pid)

        :param pid: The pid of the active process to get
        :return: The active process instance
        """
        return MONITOR.get_process(pid)

    @abstractmethod
    def has_finished(self, pid):
        pass

    @abstractmethod
    def get_output(self, pid, port_name):
        raise NotImplementedError("Cannot get process output")

    @abstractmethod
    def get_outputs(self, pid):
        """
        Get the outputs from a process.

        :param pid: The process id
        :return: A dictionary containing label: value entries.
        """
        raise NotImplementedError("Cannot get process outputs")


_process_database = None


def get_db():
    global _process_database
    return _process_database


def set_db(process_database):
    global _process_database
    _process_database = process_database

