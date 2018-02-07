from abc import ABCMeta, abstractmethod
import collections
import copy
import errno
import fnmatch
import inspect
import os
import pickle
from future.utils import with_metaclass

from . import class_loader
from . import utils
from . import base
from .base import super_check

__all__ = ['Bundle', 'Persister', 'PicklePersister', 'auto_persist', 'Savable']

PersistedCheckpoint = collections.namedtuple('PersistedCheckpoint', ['pid', 'tag'])


class Bundle(dict):
    _class_loader = class_loader.ClassLoader()

    @classmethod
    def from_dict(cls, *args, **kwargs):
        self = Bundle.__new__(*args, **kwargs)
        super(Bundle, self).from_dict(*args, **kwargs)
        return self

    def __init__(self, persistable, cl=None):
        super(Bundle, self).__init__()
        if cl is not None:
            self.set_class_loader(cl)
        self['CLASS_NAME'] = utils.class_name(persistable, self._class_loader)
        self.update(persistable.save())
        # persistable.save_state(self)

    def set_class_loader(self, cl):
        self._class_loader = cl

    def unbundle(self, *args, **kwargs):
        """
        This method loads the class of the object and calls its recreate_from
        method passing the positional and keyword arguments.

        :param args: Positional arguments for recreate_from
        :param kwargs: Keyword arguments for recreate_from
        :return: An instance of the Persistable
        """
        cls = self._class_loader.load_class(self['CLASS_NAME'])
        return cls.recreate_from(self, *args, **kwargs)


class Persister(with_metaclass(ABCMeta, object)):
    @abstractmethod
    def save_checkpoint(self, process, tag=None):
        """
        Persist a Process instance

        :param process: :class:`plumpy.Process`
        :param tag: optional checkpoint identifier to allow distinguishing
            multiple checkpoints for the same process
        """
        pass

    @abstractmethod
    def load_checkpoint(self, pid, tag=None):
        """
        Load a process from a persisted checkpoint by its process id

        :param pid: the process id of the :class:`plumpy.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        :return: a bundle with the process state
        :rtype: :class:`plumpy.Bundle`
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

        :param pid: the process id of the :class:`plumpy.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        """
        pass

    @abstractmethod
    def delete_process_checkpoints(self, pid):
        """
        Delete all persisted checkpoints related to the given process id

        :param pid: the process id of the :class:`plumpy.Process`
        """
        pass


PersistedPickle = collections.namedtuple('PersistedPickle', ['checkpoint', 'bundle'])
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

        :param process: :class:`plumpy.Process`
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

        :param pid: the process id of the :class:`plumpy.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        :return: a bundle with the process state
        :rtype: :class:`plumpy.Bundle`
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

        :param pid: the process id of the :class:`plumpy.Process`
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

        :param pid: the process id of the :class:`plumpy.Process`
        """
        for checkpoint in self.get_process_checkpoints(pid):
            self.delete_checkpoint(checkpoint.pid, checkpoint.tag)


def auto_persist(*members):
    def wrapped(savable):
        if savable._auto_persist is None:
            savable._auto_persist = set()
        else:
            savable._auto_persist = set(savable._auto_persist)
        savable.auto_persist(*members)
        return savable

    return wrapped


class Savable(object):
    CLASS_NAME = 'class_name'
    META = '!!meta'
    METHOD = 'm'
    SAVABLE = 'S'
    _auto_persist = None
    _persist_configured = False

    @staticmethod
    def load(saved_state, *args, **kwargs):
        """
        Load a `Savable` from a saved instance state

        :param saved_state: The saved states
        :param args: Positional args to be passed to `load_instance_state`
        :param kwargs: Keyword args to be passed to `load_instance_state`
        :return: The loaded Savable instance
        :rtype: :class:`Savable`
        """
        return Savable.load_with_classloader(
            saved_state, class_loader.ClassLoader(), *args, **kwargs)

    @staticmethod
    def load_with_classloader(saved_state, class_loader_, *args, **kwargs):
        try:
            load_cls = class_loader_.load_class(saved_state[Savable.CLASS_NAME])
        except KeyError:
            raise ValueError("Class name not found in saved state")
        else:
            return load_cls.recreate_from(saved_state, *args, **kwargs)

    @classmethod
    def auto_persist(cls, *members):
        if cls._auto_persist is None:
            cls._auto_persist = set()
        cls._auto_persist.update(members)

    @classmethod
    def persist(cls):
        pass

    @classmethod
    def recreate_from(cls, saved_state, *args, **kwargs):
        obj = cls.__new__(cls)
        base.call_with_super_check(obj.load_instance_state, saved_state, *args, **kwargs)
        return obj

    @super_check
    def load_instance_state(self, saved_state, *args, **kwargs):
        if self._auto_persist is not None:
            self.load_members(self._auto_persist, saved_state)

    @super_check
    def save_instance_state(self, out_state):
        self._ensure_persist_configured()
        out_state[self.META] = {}
        if self._auto_persist is not None:
            self.save_members(self._auto_persist, out_state)

    def save(self, include_class_name=True):
        out_state = {}
        if include_class_name:
            out_state[self.CLASS_NAME] = utils.class_name(self)
        base.call_with_super_check(self.save_instance_state, out_state)
        return out_state

    def save_members(self, members, out_state):
        for member in members:
            value = getattr(self, member)
            if inspect.ismethod(value):
                if value.__self__ is not self:
                    raise TypeError("Cannot persist methods of other classes")
                out_state[self.META] = 'method'
                value = value.__name__
            elif isinstance(value, Savable):
                value = value.save()
            else:
                value = copy.deepcopy(value)
            out_state[member] = value

    def load_members(self, members, saved_state):
        for member in members:
            setattr(self, member, self._get_value(saved_state, member))

    def _ensure_persist_configured(self):
        if not self._persist_configured:
            self.persist()
            self._persist_configured = True

    def _get_value(self, saved_state, name):
        value = saved_state[name]
        if name in saved_state[self.META]:
            typ = saved_state[self.META][name]
            if typ == self.METHOD:
                value = getattr(self, value)
            elif type == self.SAVABLE:
                value = Savable.load(value)

        return value
