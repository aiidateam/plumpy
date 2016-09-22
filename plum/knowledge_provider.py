
from abc import ABCMeta, abstractmethod
from plum.process_monitor import MONITOR


class NotKnown(Exception):
    def __init__(self, msg=None):
        super(NotKnown, self).__init__(msg)


class KnowledgeProvider(object):
    __metaclass__ = ABCMeta

    def has_finished(self, pid):
        raise NotKnown()

    def get_input(self, pid, port_name):
        """
        Get the input of a process on the given port name.

        :param pid: The process id.
        :param port_name: The name of the port.
        :return: The corresponding input value.
        :raises: NotKnown
        """
        raise NotKnown()

    def get_inputs(self, pid):
        """
        Get all the inputs for a given process.

        :param pid: The process id.
        :return: A dictionary of the corresponding port names and input values.
        :rtype: dict
        :raises: NotKnown
        """
        raise NotKnown()

    def get_output(self, pid, port_name):
        """
        Get the output of a process on the given port name.

        :param pid: The process id.
        :param port_name: The name of the port.
        :return: The corresponding output value
        :raises: NotKnown
        """
        raise NotKnown()

    def get_outputs(self, pid):
        """
        Get all the outputs from a process.

        :param pid: The process id
        :return: A dictionary containing label: value entries.
        :raises: NotKnown
        """
        raise NotKnown()

    def get_pids_from_classname(self, classname):
        """
        Get all the process ids for a specific process class.

        :param classname: The fully qualified classname of the process.
        :return: A list of pids.
        :raises: NotKnown
        """
        raise NotKnown()


_global_provider = None


def get_global_provider():
    """
    Get the global knowledge provider if set.

    :return: The global knowledge provider, or None.
    :rtype: :class:`KnowledgeProvider`
    """
    global _global_provider
    return _global_provider


def set_global_provider(process_database):
    global _global_provider
    _global_provider = process_database

