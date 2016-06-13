
from abc import ABCMeta, abstractmethod


class ProcessRegistry(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_running_process(self, pid):
        pass

    @abstractmethod
    def register_running_process(self, process):
        pass

    @abstractmethod
    def is_finished(self, pid):
        pass

    @abstractmethod
    def get_output(self, pid, port_name):
        pass

    @abstractmethod
    def get_outputs(self, pid):
        """
        Get the outputs from a finished process.  Precondition:
        is_finished(pid) is True

        :param pid: The process id
        :return: A dictionary containing label: value entries.
        """
        pass
