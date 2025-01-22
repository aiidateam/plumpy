# -*- coding: utf-8 -*-
from __future__ import annotations

import abc
import asyncio
import collections
import copy
import errno
import fnmatch
import inspect
import os
import pickle
from types import MethodType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Generator,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Protocol,
    Self,
    Type,
    TypeVar,
    cast,
    runtime_checkable,
)

import yaml

from . import futures, loaders, utils
from .utils import PID_TYPE, SAVED_STATE_TYPE

PersistedCheckpoint = collections.namedtuple('PersistedCheckpoint', ['pid', 'tag'])

if TYPE_CHECKING:
    from .processes import Process


class LoadSaveContext:
    def __init__(self, loader: Optional[loaders.ObjectLoader] = None, **kwargs: Any) -> None:
        self._values = dict(**kwargs)
        self.loader = loader

    def __getattr__(self, item: str) -> Any:
        try:
            return self._values[item]
        except KeyError as exc:
            raise AttributeError(f"item '{item}' not found in the runtime context when load") from exc

    def __iter__(self) -> Iterable[Any]:
        return self._value.__iter__()

    def __contains__(self, item: Any) -> bool:
        return self._values.__contains__(item)

    def copyextend(self, **kwargs: Any) -> 'LoadSaveContext':
        """Add additional information to the context by making a copy with the new values"""
        extended = self._values.copy()
        extended.update(kwargs)
        loader = extended.pop('loader', self.loader)
        return LoadSaveContext(loader=loader, **extended)


class Bundle(dict):
    def __init__(self, savable: 'Savable', loader: loaders.ObjectLoader | None = None, dereference: bool = False):
        """
        Create a bundle from a savable.  Optionally keep information about the
        class loader that can be used to load the classes in the bundle.

        :param savable: The savable object to bundle
        :param loader: The optional object loader to use
        :param dereference: Remove refrences from the data, by deep copying

        """
        super().__init__()
        if dereference:
            self.update(copy.deepcopy(savable.save(loader)))
        else:
            self.update(savable.save(loader))

    def unbundle(self, load_context: LoadSaveContext | None = None) -> 'Savable':
        """
        This method loads the class of the object and calls its recreate_from
        method passing the positional and keyword arguments.

        :param load_context: The optional load context
        :return: An instance of the Savable

        """
        return load(self, load_context)


def load(saved_state: SAVED_STATE_TYPE, load_context: LoadSaveContext | None = None) -> 'Savable':
    """
    Load a `Savable` from a saved instance state.  The load context is a way of passing
    runtime data to the object being loaded.

    :param saved_state: The saved state
    :param load_context: Additional runtime state that can be passed into when loading.
        The type and content (if any) is completely user defined
    :return: The loaded Savable instance

    """
    load_context = ensure_object_loader(load_context, saved_state)
    assert load_context.loader is not None  # required for type checking
    try:
        class_name = SaveUtil.get_class_name(saved_state)
        load_cls: Savable = load_context.loader.load_object(class_name)
    except KeyError:
        raise ValueError('Class name not found in saved state')
    else:
        return load_cls.recreate_from(saved_state, load_context)


_BUNDLE_TAG = '!plumpy:Bundle'


def _bundle_representer(dumper: yaml.Dumper, node: Any) -> Any:
    return dumper.represent_mapping(_BUNDLE_TAG, node)


def _bundle_constructor(loader: yaml.Loader, data: Any) -> Generator[Bundle, None, None]:
    result = Bundle.__new__(Bundle)
    yield result
    mapping = loader.construct_mapping(data)
    result.update(mapping)


yaml.add_representer(Bundle, _bundle_representer)
yaml.add_constructor(_BUNDLE_TAG, _bundle_constructor)  # type: ignore[arg-type]


class Persister(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def save_checkpoint(self, process: 'Process', tag: Optional[str] = None) -> None:
        """
        Persist a Process instance

        :param process: :class:`plumpy.Process`
        :param tag: optional checkpoint identifier to allow distinguishing
            multiple checkpoints for the same process
        :raises: :class:`plumpy.PersistenceError` Raised if there was a problem saving the checkpoint
        """

    @abc.abstractmethod
    def load_checkpoint(self, pid: PID_TYPE, tag: Optional[str] = None) -> Bundle:
        """
        Load a process from a persisted checkpoint by its process id

        :param pid: the process id of the :class:`plumpy.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        :return: a bundle with the process state

        :raises: :class:`plumpy.PersistenceError` Raised if there was a problem loading the checkpoint
        """

    @abc.abstractmethod
    def get_checkpoints(self) -> List[PersistedCheckpoint]:
        """
        Return a list of all the current persisted process checkpoints
        with each element containing the process id and optional checkpoint tag

        :return: list of PersistedCheckpoint
        """

    @abc.abstractmethod
    def get_process_checkpoints(self, pid: PID_TYPE) -> List[PersistedCheckpoint]:
        """
        Return a list of all the current persisted process checkpoints for the
        specified process with each element containing the process id and
        optional checkpoint tag

        :param pid: the process pid
        :return: list of PersistedCheckpoint tuples
        """

    @abc.abstractmethod
    def delete_checkpoint(self, pid: PID_TYPE, tag: Optional[str] = None) -> None:
        """
        Delete a persisted process checkpoint. No error will be raised if
        the checkpoint does not exist

        :param pid: the process id of the :class:`plumpy.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        """

    @abc.abstractmethod
    def delete_process_checkpoints(self, pid: PID_TYPE) -> None:
        """
        Delete all persisted checkpoints related to the given process id

        :param pid: the process id of the :class:`plumpy.Process`
        """


PersistedPickle = collections.namedtuple('PersistedPickle', ['checkpoint', 'bundle'])
_PICKLE_SUFFIX = 'pickle'


class PicklePersister(Persister):
    """
    Implementation of the abstract Persister class that stores Process states
    in pickles on a filesystem.
    """

    def __init__(self, pickle_directory: str):
        """
        Instantiate a PicklePersister object that will persist processes by
        writing their bundles to a pickle in a directory specified by the
        argument 'pickle_directory'

        :param pickle_directory: the full path to the directory where pickles will be written
        """
        super().__init__()

        try:
            PicklePersister.ensure_pickle_directory(pickle_directory)
        except OSError:
            raise ValueError(f'failed to create the pickle directory at {pickle_directory}')

        self._pickle_directory = pickle_directory

    @staticmethod
    def ensure_pickle_directory(dirpath: str) -> None:
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
    def load_pickle(filepath: str) -> 'PersistedPickle':
        """
        Load a pickle from disk

        :param filepath: absolute filepath to the pickle
        :returns: the loaded pickle

        """
        with open(filepath, 'r+b') as handle:
            persisted_pickle = pickle.load(handle)

        return persisted_pickle

    @staticmethod
    def pickle_filename(pid: PID_TYPE, tag: Optional[str] = None) -> str:
        """
        Returns the relative filepath of the pickle for the given process id
        and optional checkpoint tag
        """
        if tag is not None:
            filename = f'{pid}.{tag}.{_PICKLE_SUFFIX}'
        else:
            filename = f'{pid}.{_PICKLE_SUFFIX}'

        return filename

    def _pickle_filepath(self, pid: PID_TYPE, tag: Optional[str] = None) -> str:
        """
        Returns the full filepath of the pickle for the given process id
        and optional checkpoint tag
        """
        return os.path.join(self._pickle_directory, PicklePersister.pickle_filename(pid, tag))

    def save_checkpoint(self, process: 'Process', tag: Optional[str] = None) -> None:
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

    def load_checkpoint(self, pid: PID_TYPE, tag: Optional[str] = None) -> Bundle:
        """
        Load a process from a persisted checkpoint by its process id

        :param pid: the process id of the :class:`plumpy.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        :return: a bundle with the process state

        """
        filepath = self._pickle_filepath(pid, tag)
        checkpoint = PicklePersister.load_pickle(filepath)

        return checkpoint.bundle

    def get_checkpoints(self) -> List[PersistedCheckpoint]:
        """
        Return a list of all the current persisted process checkpoints
        with each element containing the process id and optional checkpoint tag

        :return: list of PersistedCheckpoint
        """
        checkpoints = []
        file_pattern = f'*.{_PICKLE_SUFFIX}'

        for _, _, files in os.walk(self._pickle_directory):
            for filename in fnmatch.filter(files, file_pattern):
                filepath = os.path.join(self._pickle_directory, filename)
                persisted_pickle = PicklePersister.load_pickle(filepath)
                checkpoints.append(persisted_pickle.checkpoint)

        return checkpoints

    def get_process_checkpoints(self, pid: PID_TYPE) -> List[PersistedCheckpoint]:
        """
        Return a list of all the current persisted process checkpoints for the
        specified process with each element containing the process id and
        optional checkpoint tag

        :param pid: the process pid
        :return: list of PersistedCheckpoint
        """
        return [c for c in self.get_checkpoints() if c.pid == pid]

    def delete_checkpoint(self, pid: PID_TYPE, tag: Optional[str] = None) -> None:
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

    def delete_process_checkpoints(self, pid: PID_TYPE) -> None:
        """
        Delete all persisted checkpoints related to the given process id

        :param pid: the process id of the :class:`plumpy.Process`
        """
        for checkpoint in self.get_process_checkpoints(pid):
            self.delete_checkpoint(checkpoint.pid, checkpoint.tag)


class InMemoryPersister(Persister):
    """Mainly to be used in testing/debugging"""

    def __init__(self, loader: Optional[loaders.ObjectLoader] = None) -> None:
        super().__init__()
        self._checkpoints: Dict[PID_TYPE, Dict[Optional[str], Bundle]] = {}
        self._save_context = LoadSaveContext(loader=loader)

    def save_checkpoint(self, process: 'Process', tag: Optional[str] = None) -> None:
        self._checkpoints.setdefault(process.pid, {})[tag] = Bundle(
            process, self._save_context.loader, dereference=True
        )

    def load_checkpoint(self, pid: PID_TYPE, tag: Optional[str] = None) -> Bundle:
        return self._checkpoints[pid][tag]

    def get_checkpoints(self) -> List[PersistedCheckpoint]:
        cps = []
        for pid in self._checkpoints:
            cps.extend(self.get_process_checkpoints(pid))
        return cps

    def get_process_checkpoints(self, pid: PID_TYPE) -> List[PersistedCheckpoint]:
        cps = []
        try:
            for tag, _ in self._checkpoints[pid].items():
                cps.append(PersistedCheckpoint(pid, tag))
        except KeyError:
            pass
        return cps

    def delete_checkpoint(self, pid: PID_TYPE, tag: Optional[str] = None) -> None:
        try:
            del self._checkpoints[pid][tag]
        except KeyError:
            pass

    def delete_process_checkpoints(self, pid: PID_TYPE) -> None:
        if pid in self._checkpoints:
            del self._checkpoints[pid]


def ensure_object_loader(context: Optional['LoadSaveContext'], saved_state: SAVED_STATE_TYPE) -> 'LoadSaveContext':
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

    # 2) Try getting from saved_state
    default_loader = loaders.get_object_loader()
    try:
        loader_identifier = SaveUtil.get_custom_meta(saved_state, META__OBJECT_LOADER)
    except ValueError:
        # 3) Fall back to default
        loader = default_loader
    else:
        loader = default_loader.load_object(loader_identifier)

    return context.copyextend(loader=loader)


META: str = '!!meta'
META__CLASS_NAME: str = 'class_name'
META__OBJECT_LOADER: str = 'object_loader'
META__USER: str = 'user'
META__TYPES: str = 'types'
META__TYPE__METHOD: str = 'm'
META__TYPE__SAVABLE: str = 'S'


class SaveUtil:
    @staticmethod
    def set_custom_meta(out_state: SAVED_STATE_TYPE, name: str, value: Any) -> None:
        user_dict = SaveUtil.get_create_meta(out_state).setdefault(META__USER, {})
        user_dict[name] = value

    @staticmethod
    def get_custom_meta(saved_state: SAVED_STATE_TYPE, name: str) -> Any:
        try:
            return saved_state[META][name]
        except KeyError:
            raise ValueError(f"Unknown meta key '{name}'")

    @staticmethod
    def get_create_meta(out_state: SAVED_STATE_TYPE) -> Dict[str, Any]:
        return out_state.setdefault(META, {})

    @staticmethod
    def set_class_name(out_state: SAVED_STATE_TYPE, name: str) -> None:
        SaveUtil.get_create_meta(out_state)[META__CLASS_NAME] = name

    @staticmethod
    def get_class_name(saved_state: SAVED_STATE_TYPE) -> str:
        return SaveUtil.get_create_meta(saved_state)[META__CLASS_NAME]

    @staticmethod
    def set_meta_type(out_state: SAVED_STATE_TYPE, name: str, type_spec: Any) -> None:
        type_dict = SaveUtil.get_create_meta(out_state).setdefault(META__TYPES, {})
        type_dict[name] = type_spec

    @staticmethod
    def get_meta_type(saved_state: SAVED_STATE_TYPE, name: str) -> Any:
        try:
            return saved_state[META][META__TYPES][name]
        except KeyError:
            pass


T = TypeVar('T', bound='Savable')


@runtime_checkable
class Savable(Protocol):
    @classmethod
    def recreate_from(cls: type[T], saved_state: SAVED_STATE_TYPE, load_context: LoadSaveContext | None = None) -> T:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        ...

    def save(self, loader: loaders.ObjectLoader | None = None) -> SAVED_STATE_TYPE: ...


@runtime_checkable
class SavableWithAutoPersist(Savable, Protocol):
    _auto_persist: ClassVar[set[str]] = set()


def auto_save(obj: Savable, loader: loaders.ObjectLoader | None = None) -> SAVED_STATE_TYPE:
    out_state: SAVED_STATE_TYPE = {}

    default_loader = loaders.get_object_loader()
    if loader is None:
        loader = default_loader

    # If the user has specified a class loader saver the loader in the saved state, or save the default loader
    loader_class = default_loader.identify_object(loader.__class__)
    SaveUtil.set_custom_meta(out_state, META__OBJECT_LOADER, loader_class)

    # Save object class name
    SaveUtil.set_class_name(out_state, loader.identify_object(obj.__class__))

    if isinstance(obj, SavableWithAutoPersist):
        for member in obj._auto_persist:
            value = getattr(obj, member)
            if inspect.ismethod(value):
                if value.__self__ is not obj:
                    raise TypeError('Cannot persist methods of other classes')
                SaveUtil.set_meta_type(out_state, member, META__TYPE__METHOD)
                value = value.__name__
            elif isinstance(value, Savable) and not isinstance(value, type):
                # persist for a savable obj, call `save` method of obj.
                # the rhs branch is for when value is a Savable class, it is true runtime check
                # of lhs condition.
                SaveUtil.set_meta_type(out_state, member, META__TYPE__SAVABLE)
                value = value.save()
            else:
                value = copy.deepcopy(value)
            out_state[member] = value

    return out_state


def load_auto_persist_params(
    obj: SavableWithAutoPersist, saved_state: SAVED_STATE_TYPE, load_context: LoadSaveContext | None
) -> None:
    for member in obj._auto_persist:
        value = saved_state[member]

        typ = SaveUtil.get_meta_type(saved_state, member)
        if typ == META__TYPE__METHOD:
            value = getattr(obj, value)
        elif typ == META__TYPE__SAVABLE:
            value = load(value, load_context)

        setattr(obj, member, value)


def auto_load(cls: type[T], saved_state: SAVED_STATE_TYPE, load_context: LoadSaveContext | None = None) -> T:
    obj = cls.__new__(cls)

    if isinstance(obj, SavableWithAutoPersist):
        load_auto_persist_params(obj, saved_state, load_context)

    return obj


def auto_persist(*members: str) -> Callable[[type[T]], type[T]]:
    def wrapped(cls: type[T]) -> type[T]:
        if not hasattr(cls, '_auto_persist') or cls._auto_persist is None:
            cls._auto_persist = set()  # type: ignore[attr-defined]
        else:
            cls._auto_persist = set(cls._auto_persist)

        cls._auto_persist.update(members)  # type: ignore[attr-defined]
        # XXX: validate on `save` and `recreate_from` method??
        return cls

    return wrapped


@auto_persist('_state', '_result')
class SavableFuture(futures.Future):
    """
    A savable future.

    .. note: This does not save any assigned done callbacks.
    """

    def save(self, loader: loaders.ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = auto_save(self, loader)
        if self.done() and self.exception() is not None:
            out_state['exception'] = self.exception()

        return out_state

    @classmethod
    def recreate_from(cls, saved_state: SAVED_STATE_TYPE, load_context: Optional[LoadSaveContext] = None) -> Self:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)

        try:
            loop = load_context.loop
        except AttributeError:
            loop = asyncio.get_event_loop()

        state = saved_state['_state']

        if state == asyncio.futures._PENDING:  # type: ignore
            obj = cls(loop=loop)

        if state == asyncio.futures._FINISHED:  # type: ignore
            obj = cls(loop=loop)
            result = saved_state['_result']

            try:
                exception = saved_state['exception']
                obj.set_exception(exception)
            except KeyError:
                obj.set_result(result)

        if state == asyncio.futures._CANCELLED:  # type: ignore
            obj = cls(loop=loop)
            obj.cancel()

        # ## XXX: load_instance_state: test not cover
        # auto_load(obj, saved_state, load_context)
        #
        # if obj._callbacks:
        #     # typing says asyncio.Future._callbacks needs to be called, but in the python 3.7 code it is a simple list
        #     for callback in obj._callbacks:
        #         obj.remove_done_callback(callback)  # type: ignore[arg-type]
        # ## UNTILHERE XXX:

        return obj
