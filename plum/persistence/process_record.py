# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod, abstractproperty


class ProcessRecord(object):
    __metaclass__ = ABCMeta

    @abstractproperty
    def pid(self):
        """
        The id assigned to this process.
        :return: The process id.
        """
        pass

    @abstractproperty
    def process_class(self):
        """
        The fully qualified string for the name of the process class.
        :return:
        """
        pass

    @abstractproperty
    def inputs(self):
        pass

    @abstractproperty
    def last_saved(self):
        """
        The point this record was last saved to backing storage.
        :return:
        """
        pass

    @abstractproperty
    def instance_state(self):
        """
        A MutableMapping that can contain a custom state for this record
        :return:
        """
        pass

    @abstractmethod
    def save(self):
        pass

    @abstractmethod
    def delete(self):
        """
        Delete the persistent record backing this class.
        """
        pass

    @abstractmethod
    def create_child(self, child_process, inputs):
        """
        Create persistence record for a child process.

        :param child_process: The child process
        :param inputs: Its inputs
        :return: A child ProcessRecord
        """
        pass

    @abstractmethod
    def remove_child(self, pid):
        """
        Remove a child record.

        :param pid: The process ID of the child to remove.
        """
        pass

    @abstractmethod
    def has_child(self, pid):
        """
        Check if this process has a child process record
        :param pid: The process ID of the child
        :return: True if the process record is a child, False otherwise
        """
        pass


