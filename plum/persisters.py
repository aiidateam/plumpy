from abc import ABCMeta, abstractmethod
from collections import namedtuple
import errno
import fnmatch
import os
import pickle
from future.utils import with_metaclass

from . import utils

__all__ = ['Bundle', 'Persister', 'PicklePersister']

PersistedCheckpoint = namedtuple('PersistedCheckpoint', ['pid', 'tag'])


class Bundle(dict):
    @classmethod
    def from_dict(cls, *args, **kwargs):
        self = Bundle.__new__(*args, **kwargs)
        super(Bundle, self).from_dict(*args, **kwargs)
        return self

    def __init__(self, persistable):
        super(Bundle, self).__init__()
        self['CLASS_NAME'] = utils.class_name(persistable)
        persistable.save_state(self)

    def unbundle(self, loop=None):
        cls = utils.load_object(self['CLASS_NAME'])
        return cls.recreate_from(self, loop)


class Persister(with_metaclass(ABCMeta, object)):
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
        :rtype: :class:`plum.Bundle`
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


PersistedPickle = namedtuple('PersistedPickle', ['checkpoint', 'bundle'])
_PICKLE_SUFFIX = 'pickle'


class PicklePersister(Persister):
    """
    Implementation of the abstract Persister class that stores Process states
    in pickles on a filesystem.
    """

    def __init__(self, pickle_directory):
        """
        Instantiate a PicklePersister object that will persist processes by
        writing their bundles to a pickle in a directory specified by the
        argument 'pickle_directory'

        :param pickle_directory: the full path to the directory where pickles will be written
        """
        super(PicklePersister, self).__init__()

        try:
            PicklePersister.ensure_pickle_directory(pickle_directory)
        except OSError as exception:
            raise ValueError('failed to create the pickle directory at {}'.format(pickle_directory))

        self._pickle_directory = pickle_directory

    @staticmethod
    def ensure_pickle_directory(dirpath):
        """
        Will attempt to create the directory at dirpath and raise if it fails, except
        if the exception arose because the directory already existed
        """
        try:
            os.makedirs(dirpath)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    @staticmethod
    def load_pickle(filepath):
        """
        Load a pickle from disk

        :param filepath: absolute filepath to the pickle
        :returns: the loaded pickle
        :rtype: PersistedPickle
        """
        with open(filepath, 'r+b') as handle:
            persisted_pickle = pickle.load(handle)

        return persisted_pickle

    @staticmethod
    def pickle_filename(pid, tag=None):
        """
        Returns the relative filepath of the pickle for the given process id
        and optional checkpoint tag
        """
        if tag is not None:
            filename = '{}.{}.{}'.format(pid, tag, _PICKLE_SUFFIX)
        else:
            filename = '{}.{}'.format(pid, _PICKLE_SUFFIX)

        return filename

    def _pickle_filepath(self, pid, tag=None):
        """
        Returns the full filepath of the pickle for the given process id
        and optional checkpoint tag
        """
        return os.path.join(self._pickle_directory, PicklePersister.pickle_filename(pid, tag))

    def save_checkpoint(self, process, tag=None):
        """
        Persist a process to a pickle on disk

        :param process: :class:`plum.process.Process`
        :param tag: optional checkpoint identifier to allow distinguishing
            multiple checkpoints for the same process
        """
        bundle = Bundle(process)
        checkpoint = PersistedCheckpoint(process.pid, tag)
        persisted_pickle = PersistedPickle(checkpoint, bundle)

        with open(self._pickle_filepath(process.pid, tag), 'w+b') as handle:
            pickle.dump(persisted_pickle, handle)

    def load_checkpoint(self, pid, tag=None):
        """
        Load a process from a persisted checkpoint by its process id

        :param pid: the process id of the :class:`plum.process.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        :return: a bundle with the process state
        :rtype: :class:`plum.Bundle`
        """
        filepath = self._pickle_filepath(pid, tag)
        checkpoint = PicklePersister.load_pickle(filepath)

        return checkpoint.bundle

    def get_checkpoints(self):
        """
        Return a list of all the current persisted process checkpoints
        with each element containing the process id and optional checkpoint tag

        :return: list of PersistedCheckpoint tuples
        """
        checkpoints = []
        file_pattern = '*.{}'.format(_PICKLE_SUFFIX)

        for subdir, dirs, files in os.walk(self._pickle_directory):
            for filename in fnmatch.filter(files, file_pattern):
                filepath = os.path.join(self._pickle_directory, filename)
                persisted_pickle = PicklePersister.load_pickle(filepath)
                checkpoints.append(persisted_pickle.checkpoint)

        return checkpoints

    def get_process_checkpoints(self, pid):
        """
        Return a list of all the current persisted process checkpoints for the
        specified process with each element containing the process id and
        optional checkpoint tag

        :param pid: the process pid
        :return: list of PersistedCheckpoint tuples
        """
        return [c for c in self.get_checkpoints() if c.pid == pid]

    def delete_checkpoint(self, pid, tag=None):
        """
        Delete a persisted process checkpoint. No error will be raised if
        the checkpoint does not exist

        :param pid: the process id of the :class:`plum.process.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        """
        pickle_filepath = self._pickle_filepath(pid, tag)

        try:
            os.remove(pickle_filepath)
        except OSError:
            pass

    def delete_process_checkpoints(self, pid):
        """
        Delete all persisted checkpoints related to the given process id

        :param pid: the process id of the :class:`plum.process.Process`
        """
        for checkpoint in self.get_process_checkpoints(pid):
            self.delete_checkpoint(checkpoint.pid, checkpoint.tag)
