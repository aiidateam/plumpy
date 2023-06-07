# -*- coding: utf-8 -*-
import asyncio
from collections import deque
from collections.abc import Mapping
import functools
import importlib
import inspect
import logging
import types
from typing import Set  # pylint: disable=unused-import
from typing import TYPE_CHECKING, Any, Callable, Hashable, Iterator, List, MutableMapping, Optional, Tuple, Type

from . import lang
from .settings import check_override, check_protected

if TYPE_CHECKING:
    from .processes import ProcessListener  # pylint: disable=cyclic-import

__all__ = ['AttributesDict']

protected = lang.protected(check=check_protected)  # pylint: disable=invalid-name
override = lang.override(check=check_override)  # pylint: disable=invalid-name

_LOGGER = logging.getLogger(__name__)

SAVED_STATE_TYPE = MutableMapping[str, Any]  # pylint: disable=invalid-name
PID_TYPE = Hashable  # pylint: disable=invalid-name


class EventHelper:

    def __init__(self, listener_type: 'Type[ProcessListener]'):
        assert listener_type is not None, 'Must provide valid listener type'

        self._listener_type = listener_type
        self._listeners: 'Set[ProcessListener]' = set()

    def add_listener(self, listener: 'ProcessListener') -> None:
        assert isinstance(listener, self._listener_type), 'Listener is not of right type'
        self._listeners.add(listener)

    def remove_listener(self, listener: 'ProcessListener') -> None:
        self._listeners.discard(listener)

    def remove_all_listeners(self) -> None:
        self._listeners.clear()

    @property
    def listeners(self) -> 'Set[ProcessListener]':
        return self._listeners

    def fire_event(self, event_function: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        """Call an event method on all listeners.

        :param event_function: the method of the ProcessListener
        :param args: arguments to pass to the method
        :param kwargs: keyword arguments to pass to the method

        """
        if event_function is None:
            raise ValueError('Must provide valid event method')

        # Make a copy of the list for iteration just in case it changes in a callback
        for listener in list(self.listeners):
            try:
                getattr(listener, event_function.__name__)(*args, **kwargs)
            except Exception as exception:  # pylint: disable=broad-except
                _LOGGER.error("Listener '%s' produced an exception:\n%s", listener, exception)


class Frozendict(Mapping):
    """
    An immutable wrapper around dictionaries that implements the complete :py:class:`collections.abc.Mapping`
    interface. It can be used as a drop-in replacement for dictionaries where immutability is desired.

    Adapted from: slezica/python-frozendict
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._dict = dict(*args, **kwargs)
        self._hash: Optional[int] = None

    def __getitem__(self, key: str) -> Any:
        return self._dict[key]

    def __contains__(self, key: Any) -> bool:
        return key in self._dict

    def copy(self, **add_or_replace: Any) -> 'Frozendict':
        return self.__class__(self, **add_or_replace)

    def __iter__(self) -> Iterator[str]:
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} {self._dict!r}>'

    def __hash__(self) -> int:
        if self._hash is None:
            hashed = 0
            for key, value in self._dict.items():
                hashed ^= hash((key, value))
            self._hash = hashed
        return self._hash


class AttributesFrozendict(Frozendict):

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._initialised: bool = True

    def __getattr__(self, attr: str) -> Any:
        """
        Read a key as an attribute. Raise AttributeError on missing key.
        Called only for attributes that do not exist.
        """
        # This attribute is looked for by pickle when deserialising.  At this point
        # the object is not yet constructed and so accessing any members is
        # dangerous and often causes infinite recursion so I have to guard like this.
        if attr == '__setstate__':
            raise AttributeError()
        try:
            return self[attr]
        except KeyError:
            errmsg = f"'{self.__class__.__name__}' object has no attribute '{attr}'"
            raise AttributeError(errmsg)

    def __dir__(self) -> List[str]:
        """
        So we get tab completion.
        :return: The keys of the dict
        """
        return list(self.keys())


class AttributesDict(types.SimpleNamespace):
    """Works like a dictionary, but items can also be added / accessed as attributes.

    For example::

        dct = AttributeDict()
        dct["key1"] = "value"
        dct.key2 = "value"

    """

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)

    def __getitem__(self, item: str) -> Any:
        try:
            return getattr(self, item)
        except AttributeError:
            raise KeyError(f"No key '{item}'")

    def __delitem__(self, item: str) -> None:
        return delattr(self, item)

    def setdefault(self, key: str, value: Any) -> Any:
        return self.__dict__.setdefault(key, value)

    def get(self, *args: Any, **kwargs: Any) -> Any:
        return self.__dict__.get(*args, **kwargs)


def load_function(name: str, instance: Optional[Any] = None) -> Callable[..., Any]:
    obj = load_object(name)
    if inspect.ismethod(obj):
        if instance is not None:
            return obj.__get__(instance, instance.__class__)  # type: ignore[attr-defined]

        return obj

    if inspect.isfunction(obj):
        return obj

    raise ValueError(f"Invalid function name '{name}'")


def load_object(fullname: str) -> Any:
    """
    Load a class from a string
    """
    obj, remainder = load_module(fullname)

    # Finally, retrieve the object
    for name in remainder:
        try:
            obj = getattr(obj, name)
        except AttributeError:
            raise ValueError(f"Could not load object corresponding to '{fullname}'")

    return obj


def load_module(fullname: str) -> Tuple[types.ModuleType, deque]:
    parts = fullname.split('.')

    # Try to find the module, working our way from the back
    mod = None
    remainder: deque = deque()
    for _ in range(len(parts)):
        try:
            mod = importlib.import_module('.'.join(parts))
            break
        except ImportError:
            remainder.appendleft(parts.pop())

    if mod is None:
        raise ValueError(f"Could not load a module corresponding to '{fullname}'")

    return mod, remainder


def type_check(obj: Any, expected_type: Type) -> None:
    if not isinstance(obj, expected_type):
        raise TypeError(f"Got object of type '{type(obj)}' when expecting '{expected_type}'")


def ensure_coroutine(coro_or_fn: Any) -> Callable[..., Any]:
    """
    Ensure that the given function ``fct`` is a coroutine

    If the passed function is not already a coroutine, it will be made to be a coroutine

    :param fct: the function
    :returns: the coroutine
    """
    if asyncio.iscoroutinefunction(coro_or_fn):
        return coro_or_fn

    if asyncio.iscoroutinefunction(coro_or_fn.__call__):
        return coro_or_fn

    if callable(coro_or_fn):
        if inspect.isclass(coro_or_fn):
            coro_or_fn = coro_or_fn.__call__

        @functools.wraps(coro_or_fn)
        async def wrap(*args: Any, **kwargs: Any) -> Callable[..., Any]:
            return coro_or_fn(*args, **kwargs)

        return wrap

    raise TypeError('coro_or_fn must be a callable')


def is_mutable_property(cls: Any, attribute: str) -> bool:
    """
    Determine whether the given attribute is a mutable property of cls. That is to say that
    the attribute corresponds to a property whose fset attribute is not None.

    :param cls: the class
    :param attribute: the attribute
    :returns: True if the attribute is a mutable property of cls
    """
    try:
        attr = getattr(cls, attribute)
    except AttributeError:
        return False

    return isinstance(attr, property) and attr.fset is not None
