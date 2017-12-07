from abc import abstractmethod, abstractproperty, ABCMeta
from collections import namedtuple
import plum.utils



PersistedCheckpoint = namedtuple('PersistedCheckpoint', ['pid', 'tag'])


class Bundle(dict):
    def __init__(self, persistable):
        super(Bundle, self).__init__()
        self['CLASS_NAME'] = plum.utils.class_name(persistable)
        persistable.save_state(self)

    def unbundle(self, loop=None):
        cls = plum.utils.load_object(self['CLASS_NAME'])
        return cls.create_from(self, loop)


class Persister(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def save_checkpoint(self, process, tag=None):
        """
        Persist a Process instance

        :param process: :class:`plum.process.Process`
        :param tag: optional checkpoint identifier to allow distinguishing
            multiple checkpoints for the same process
        """
        pass

    @abstractmethod
    def load_checkpoint(self, pid, tag=None):
        """
        Load a process from a persisted checkpoint by its process id

        :param pid: the process id of the :class:`plum.process.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        :return: a bundle with the process state
        :rtype: :class:`apricotpy.Bundle`
        """
        pass

    @abstractmethod
    def get_checkpoints(self):
        """
        Return a list of all the current persisted process checkpoints
        with each element containing the process id and optional checkpoint tag

        :return: list of PersistedCheckpoint tuples
        """
        pass

    @abstractmethod
    def get_process_checkpoints(self, pid):
        """
        Return a list of all the current persisted process checkpoints for the
        specified process with each element containing the process id and
        optional checkpoint tag

        :param pid: the process pid
        :return: list of PersistedCheckpoint tuples
        """
        pass

    @abstractmethod
    def delete_checkpoint(self, pid, tag=None):
        """
        Delete a persisted process checkpoint. No error will be raised if
        the checkpoint does not exist

        :param pid: the process id of the :class:`plum.process.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        """
        pass

    @abstractmethod
    def delete_process_checkpoints(self, pid):
        """
        Delete all persisted checkpoints related to the given process id

        :param pid: the process id of the :class:`plum.process.Process`
        """
        pass
