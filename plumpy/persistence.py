import abc
import collections
import copy
import errno
import fnmatch
import inspect
import os
import yaml
import pickle

from . import loaders
from . import futures
from . import utils
from . import base
from .base import super_check

__all__ = [
    'Bundle', 'Persister', 'PicklePersister', 'auto_persist', 'Savable', 'SavableFuture', 'LoadSaveContext',
    'PersistedCheckpoint', 'InMemoryPersister'
]

PersistedCheckpoint = collections.namedtuple('PersistedCheckpoint', ['pid', 'tag'])


class Bundle(dict):

    def __init__(self, savable, save_context=None):
        """
        Create a bundle from a savable.  Optionally keep information about the
        class loader that can be used to load the classes in the bundle.

        :param savable: The savable object to bundle
        :type savable: :class:`Savable`
        :param save_context: The optional save context to use
        :type save_context: :class:`LoadSaveContext`
        """
        super(Bundle, self).__init__()
        self.update(savable.save(save_context))

    def unbundle(self, load_context=None):
        """
        This method loads the class of the object and calls its recreate_from
        method passing the positional and keyword arguments.

        :param load_context: The optional load context
        :return: An instance of the Savable
        :rtype: :class:`Savable`
        """
        return Savable.load(self, load_context)


_BUNDLE_TAG = u'!plumpy:Bundle'


def _bundle_representer(dumper, node):
    return dumper.represent_mapping(_BUNDLE_TAG, node)


def _bundle_constructor(loader, data):
    result = Bundle.__new__(Bundle)
    yield result
    mapping = loader.construct_mapping(data)
    result.update(mapping)


yaml.add_representer(Bundle, _bundle_representer)
yaml.add_constructor(_BUNDLE_TAG, _bundle_constructor)


class Persister(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def save_checkpoint(self, process, tag=None):
        """
        Persist a Process instance

        :param process: :class:`plumpy.Process`
        :param tag: optional checkpoint identifier to allow distinguishing
            multiple checkpoints for the same process
        :raises: :class:`plumpy.PersistenceError` Raised if there was a problem saving the checkpoint
        """
        pass

    @abc.abstractmethod
    def load_checkpoint(self, pid, tag=None):
        """
        Load a process from a persisted checkpoint by its process id

        :param pid: the process id of the :class:`plumpy.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        :return: a bundle with the process state
        :rtype: :class:`plumpy.Bundle`
        :raises: :class:`plumpy.PersistenceError` Raised if there was a problem loading the checkpoint
        """
        pass

    @abc.abstractmethod
    def get_checkpoints(self):
        """
        Return a list of all the current persisted process checkpoints
        with each element containing the process id and optional checkpoint tag

        :return: list of PersistedCheckpoint tuples
        """
        pass

    @abc.abstractmethod
    def get_process_checkpoints(self, pid):
        """
        Return a list of all the current persisted process checkpoints for the
        specified process with each element containing the process id and
        optional checkpoint tag

        :param pid: the process pid
        :return: list of PersistedCheckpoint tuples
        """
        pass

    @abc.abstractmethod
    def delete_checkpoint(self, pid, tag=None):
        """
        Delete a persisted process checkpoint. No error will be raised if
        the checkpoint does not exist

        :param pid: the process id of the :class:`plumpy.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        """
        pass

    @abc.abstractmethod
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

        for _subdir, dirs, files in os.walk(self._pickle_directory):
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


class InMemoryPersister(Persister):
    """ Mainly to be used in testing/debugging """

    def __init__(self, loader=None):
        super(InMemoryPersister, self).__init__()
        self._checkpoints = {}
        self._save_context = LoadSaveContext(loader=loader)

    def save_checkpoint(self, process, tag=None):
        self._checkpoints.setdefault(process.pid, {})[tag] = Bundle(process, self._save_context)

    def load_checkpoint(self, pid, tag=None):
        return self._checkpoints[pid][tag]

    def get_checkpoints(self):
        cps = []
        for pid in self._checkpoints:
            cps.extend(self.get_process_checkpoints(pid))
        return cps

    def get_process_checkpoints(self, pid):
        cps = []
        try:
            for tag, bundle in self._checkpoints[pid].items():
                cps.append(PersistedCheckpoint(pid, tag))
        except KeyError:
            pass
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


def _ensure_object_loader(context, saved_state):
    """
    Given a LoadSaveContext this method will ensure that it has a valid class loader
    using the following priorities:
    1) The one that is already in the context
    2) One that is found in the saved state
    3) The default global class loader from loaders.get_object_loader()
    :param context:
    :param saved_state:
    :return:
    """
    if context is None:
        context = LoadSaveContext()

    assert isinstance(context, LoadSaveContext)
    if context.loader is not None:
        return context
    else:
        # 2) Try getting from saved_state
        default_loader = loaders.get_object_loader()
        try:
            loader_identifier = Savable.get_custom_meta(saved_state, META__OBJECT_LOADER)
        except ValueError:
            # 3) Fall back to default
            loader = default_loader
        else:
            loader = default_loader.load_object(loader_identifier)

        return context.copyextend(loader=loader)


class LoadSaveContext:

    def __init__(self, loader=None, **kwargs):
        self._values = dict(**kwargs)
        self.loader = loader

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
        loader = extended.pop('loader', self.loader)
        return LoadSaveContext(loader=loader, **extended)


META = '!!meta'
META__CLASS_NAME = 'class_name'
META__OBJECT_LOADER = 'object_loader'
META__USER = 'user'
META__TYPES = 'types'
META__TYPE__METHOD = 'm'
META__TYPE__SAVABLE = 'S'


class Savable:
    CLASS_NAME = 'class_name'

    _auto_persist = None
    _persist_configured = False

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
        load_context = _ensure_object_loader(load_context, saved_state)
        try:
            class_name = Savable._get_class_name(saved_state)
            load_cls = load_context.loader.load_object(class_name)
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
        :type load_context: :class:`LoadSaveContext`
        :return: The recreated instance
        :rtype: :class:`Savable`
        """
        load_context = _ensure_object_loader(load_context, saved_state)
        obj = cls.__new__(cls)
        base.call_with_super_check(obj.load_instance_state, saved_state, load_context)
        return obj

    @super_check
    def load_instance_state(self, saved_state, load_context):
        self._ensure_persist_configured()
        if self._auto_persist is not None:
            self.load_members(self._auto_persist, saved_state, load_context)

    @super_check
    def save_instance_state(self, out_state, save_context):
        self._ensure_persist_configured()
        if self._auto_persist is not None:
            self.save_members(self._auto_persist, out_state)

    def save(self, save_context=None):
        out_state = {}

        if save_context is None:
            save_context = LoadSaveContext()

        utils.type_check(save_context, LoadSaveContext)

        default_loader = loaders.get_object_loader()
        # If the user has specified a class loader, then save it in the saved state
        if save_context.loader is not None:
            loader_class = default_loader.identify_object(save_context.loader.__class__)
            Savable.set_custom_meta(out_state, META__OBJECT_LOADER, loader_class)
            loader = save_context.loader
        else:
            loader = default_loader

        Savable._set_class_name(out_state, loader.identify_object(self.__class__))
        base.call_with_super_check(self.save_instance_state, out_state, save_context)
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
            return saved_state[META][name]
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

    def save_instance_state(self, out_state, save_context):
        super(SavableFuture, self).save_instance_state(out_state, save_context)
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
