
from abc import ABCMeta


class NotKnown(Exception):
    """
    An exception raised when a knowledge provider doesn't know the answer to
    the query.  The message may indicate the reason.
    """
    pass


class KnowledgeProvider(object):
    """
    An object that can potentially supply some knowledge about a Process such
    as: has it ran, what were its inputs, what were its outputs, etc.
    """
    __metaclass__ = ABCMeta

    def has_finished(self, pid):
        """
        Has the process finished?

        :param pid: The process id.
        :return: True if finished, False otherwise.
        :rtype: bool
        :raises: ValueError
        """
        raise ValueError("Unknown pid")

    def get_input(self, pid, port_name):
        """
        Get the input of a process on the given port name.

        :param pid: The process id.
        :param port_name: The name of the port.
        :return: The corresponding input value.
        :raises: ValueError
        """
        return self.get_inputs(pid)[port_name]

    def get_inputs(self, pid):
        """
        Get all the inputs for a given process.

        :param pid: The process id.
        :return: A dictionary of the corresponding port names and input values.
        :rtype: dict
        :raises: ValueError
        """
        raise ValueError("Unknown pid")

    def get_output(self, pid, port_name):
        """
        Get the output of a process on the given port name.

        :param pid: The process id.
        :param port_name: The name of the port.
        :return: The corresponding output value
        :raises: ValueError
        """
        return self.get_outputs(pid)[port_name]

    def get_outputs(self, pid):
        """
        Get all the outputs from a process.

        :param pid: The process id
        :return: A dictionary containing label: value entries.
        :raises: ValueError
        """
        raise ValueError("Unknown pid")

    def get_pids_from_classname(self, classname):
        """
        Get all the process ids for a specific process class.

        :param classname: The fully qualified classname of the process.
        :return: A list of pids.
        :raises: ValueError
        """
        raise ValueError("Unknown classname")

