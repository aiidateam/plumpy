from abc import ABCMeta, abstractmethod
import collections
import copy
import errno
import fnmatch
import inspect
import os
import pickle
from future.utils import with_metaclass

import yaml

from . import class_loader
from . import futures
from . import utils
from . import base
from .base import super_check

__all__ = ['Bundle', 'Persister', 'PicklePersister', 'auto_persist', 'Savable', 'SavableFuture',
           'LoadContext', 'PersistedCheckpoint', 'InMemoryPersister']

PersistedCheckpoint = collections.namedtuple('PersistedCheckpoint', ['pid', 'tag'])


class Bundle(dict):
    CLASS_LOADER = 'class_loader'

    @classmethod
    def from_dict(cls, *args, **kwargs):
        self = Bundle.__new__(*args, **kwargs)
        super(Bundle, self).from_dict(*args, **kwargs)
        return self

    def __init__(self, savable, class_loader_=None):
        """
        Create a bundle from a savable.  Optionally keep information about the
        class loader that can be used to load the classes in the bundle.

        :param savable: The savable object to bundle
        :type savable: :class:`Savable`
        :param class_loader_: The optional class loader to use
        :type class_loader_: :class:`class_loader.ClassLoader`
        """
        super(Bundle, self).__init__()

        # If we have a class loader, save it in the bundle
        if class_loader_ is not None:
            Savable.set_custom_meta(self, self.CLASS_LOADER, yaml.dump(class_loader_))

        self.update(savable.save(class_loader_=class_loader_))

    def unbundle(self, load_context=None):
        """
        This method loads the class of the object and calls its recreate_from
        method passing the positional and keyword arguments.

        :param load_context: The optional load context
        :return: An instance of the Savable
        :rtype: :class:`Savable`
        """
        try:
            class_loader_dump = Savable.get_custom_meta(self, self.CLASS_LOADER)
        except ValueError:
            pass
        else:
            if load_context is None:
                load_context = LoadContext()
            load_context = load_context.copyextend(class_loader=yaml.load(class_loader_dump))

        return Savable.load(self, load_context)


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


class InMemoryPersister(with_metaclass(ABCMeta, object)):
    """ Mainly to be used in testing/debugging """

    def __init__(self, class_loader_=None):
        super(InMemoryPersister, self).__init__()
        self._checkpoints = {}
        self._class_loader = class_loader_

    def save_checkpoint(self, process, tag=None):
        self._checkpoints.setdefault(process.pid, {})[tag] = \
            Bundle(process, class_loader_=self._class_loader)

    def load_checkpoint(self, pid, tag=None):
        return self._checkpoints[pid][tag]

    def get_checkpoints(self):
        cps = []
        for pid in self._checkpoints:
            cps.extend(self.get_process_checkpoints(pid))
        return cps

    def get_process_checkpoints(self, pid):
        cps = []
        for tag, bundle in self._checkpoints[pid]:
            cps.append(PersistedCheckpoint(tag, bundle))
        return cps

    def delete_checkpoint(self, pid, tag=None):
        try:
            del self._checkpoints[pid][tag]
        except KeyError:
            pass

    def delete_process_checkpoints(self, pid):
        if pid in self._checkpoints:
            del self._checkpoints[pid]


def auto_persist(*members):
    def wrapped(savable):
        if savable._auto_persist is None:
            savable._auto_persist = set()
        else:
            savable._auto_persist = set(savable._auto_persist)
        savable.auto_persist(*members)
        return savable

    return wrapped


class LoadContext(object):
    def __init__(self, *args, **kwargs):
        self._values = dict(*args, **kwargs)

    def __getattr__(self, item):
        try:
            return self._values[item]
        except KeyError:
            raise AttributeError("item '{}' not found".format(item))

    def __iter__(self):
        return self._value.__iter__()

    def __contains__(self, item):
        return self._values.__contains__(item)

    def copyextend(self, **kwargs):
        """ Add additional information to the context by making a copy with the new values """
        extended = self._values.copy()
        extended.update(kwargs)
        return LoadContext(extended)


META = '!!meta'
META__CLASS_NAME = 'class_name'
META__USER = 'user'
META__TYPES = 'types'
META__TYPE__METHOD = 'm'
META__TYPE__SAVABLE = 'S'


class Savable(object):
    CLASS_NAME = 'class_name'

    _auto_persist = None
    _persist_configured = False

    @staticmethod
    def _ensure_load_context(load_context):
        """ Prepare a load context """
        if load_context is None:
            load_context = LoadContext()
        elif not isinstance(load_context, LoadContext):
            raise TypeError("load_context must be of type LoadContext")

        if 'class_loader' not in load_context:
            load_context = load_context.copyextend(class_loader=class_loader.get_class_loader())

        return load_context

    @staticmethod
    def load(saved_state, load_context=None):
        """
        Load a `Savable` from a saved instance state.  The load context is a way of passing
        runtime data to the object being loaded.

        :param saved_state: The saved state
        :param load_context: Additional runtime state that can be passed into when loading.
            The type and content (if any) is completely user defined
        :return: The loaded Savable instance
        :rtype: :class:`Savable`
        """
        load_context = Savable._ensure_load_context(load_context)
        try:
            class_name = Savable._get_class_name(saved_state)
            load_cls = load_context.class_loader.load_class(class_name)
        except KeyError:
            raise ValueError("Class name not found in saved state")
        else:
            return load_cls.recreate_from(saved_state, load_context)

    @classmethod
    def auto_persist(cls, *members):
        if cls._auto_persist is None:
            cls._auto_persist = set()
        cls._auto_persist.update(members)

    @classmethod
    def persist(cls):
        pass

    @classmethod
    def recreate_from(cls, saved_state, load_context=None):
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context
        :type load_context: :class:`LoadContext`
        :return: The recreated instance
        :rtype: :class:`Savable`
        """
        load_context = Savable._ensure_load_context(load_context)
        obj = cls.__new__(cls)
        base.call_with_super_check(obj.load_instance_state, saved_state, load_context)
        return obj

    @super_check
    def load_instance_state(self, saved_state, load_context):
        self._ensure_persist_configured()
        if self._auto_persist is not None:
            self.load_members(self._auto_persist, saved_state, load_context)

    @super_check
    def save_instance_state(self, out_state):
        self._ensure_persist_configured()
        if self._auto_persist is not None:
            self.save_members(self._auto_persist, out_state)

    def save(self, include_class_name=True, class_loader_=None):
        if class_loader_ is None:
            class_loader_ = class_loader.ClassLoader()
        out_state = {}
        if include_class_name:
            Savable._set_class_name(out_state, class_loader_.class_identifier(self))

        base.call_with_super_check(self.save_instance_state, out_state)
        return out_state

    def save_members(self, members, out_state):
        for member in members:
            value = getattr(self, member)
            if inspect.ismethod(value):
                if value.__self__ is not self:
                    raise TypeError("Cannot persist methods of other classes")
                Savable._set_meta_type(out_state, member, META__TYPE__METHOD)
                value = value.__name__
            elif isinstance(value, Savable):
                Savable._set_meta_type(out_state, member, META__TYPE__SAVABLE)
                value = value.save()
            else:
                value = copy.deepcopy(value)
            out_state[member] = value

    def load_members(self, members, saved_state, load_context=None):
        for member in members:
            setattr(self, member, self._get_value(saved_state, member, load_context))

    def _ensure_persist_configured(self):
        if not self._persist_configured:
            self.persist()
            self._persist_configured = True

    # region Metadata getter/setters

    @staticmethod
    def set_custom_meta(out_state, name, value):
        user_dict = Savable._get_create_meta(out_state).setdefault(META__USER, {})
        user_dict[name] = value

    @staticmethod
    def get_custom_meta(saved_state, name):
        try:
            return Savable._get_create_meta(saved_state)[name]
        except KeyError:
            raise ValueError("Unknown meta key '{}'".format(name))

    @staticmethod
    def _get_create_meta(out_state):
        return out_state.setdefault(META, {})

    @staticmethod
    def _set_class_name(out_state, name):
        Savable._get_create_meta(out_state)[META__CLASS_NAME] = name

    @staticmethod
    def _get_class_name(saved_state):
        return Savable._get_create_meta(saved_state)[META__CLASS_NAME]

    @staticmethod
    def _set_meta_type(out_state, name, type_spec):
        type_dict = Savable._get_create_meta(out_state).setdefault(META__TYPES, {})
        type_dict[name] = type_spec

    @staticmethod
    def _get_meta_type(saved_state, name):
        try:
            return saved_state[META][META__TYPES][name]
        except KeyError:
            pass

    # endregion

    def _get_value(self, saved_state, name, load_context):
        value = saved_state[name]

        typ = Savable._get_meta_type(saved_state, name)
        if typ == META__TYPE__METHOD:
            value = getattr(self, value)
        elif typ == META__TYPE__SAVABLE:
            value = Savable.load(value, load_context)

        return value


@auto_persist('_done', '_result')
class SavableFuture(futures.Future, Savable):
    """
    A savable future.

    .. note: This does not save any assigned done callbacks.
    """
    EXCEPTION = 'exception'

    def save_instance_state(self, out_state):
        super(SavableFuture, self).save_instance_state(out_state)
        if self.done() and self.exception() is not None:
            out_state[self.EXCEPTION] = self.exception()

    def load_instance_state(self, saved_state, load_context):
        super(SavableFuture, self).load_instance_state(saved_state, load_context)
        try:
            exception = saved_state[self.EXCEPTION]
            self._exc_info = (type(exception), exception, None)
        except KeyError:
            self._exc_info = None

        self._log_traceback = False
        self._tb_logger = None
        self._callbacks = []
