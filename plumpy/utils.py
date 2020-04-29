# -*- coding: utf-8 -*-
import importlib
import inspect
import logging
import threading
import types
from collections import deque, defaultdict

import tornado.gen
import frozendict

from .settings import check_protected, check_override
from . import lang

__all__ = []

protected = lang.protected(check=check_protected)  # pylint: disable=invalid-name
override = lang.override(check=check_override)  # pylint: disable=invalid-name

_LOGGER = logging.getLogger(__name__)


class EventHelper:

    def __init__(self, listener_type):
        assert listener_type is not None, 'Must provide valid listener type'

        self._listener_type = listener_type
        self._listeners = set()

    def add_listener(self, listener):
        assert isinstance(listener, self._listener_type), 'Listener is not of right type'
        self._listeners.add(listener)

    def remove_listener(self, listener):
        self._listeners.discard(listener)

    def remove_all_listeners(self):
        self._listeners.clear()

    @property
    def listeners(self):
        return self._listeners

    def fire_event(self, event_function, *args, **kwargs):
        if event_function is None:
            raise ValueError('Must provide valid event method')

        # Make a copy of the list for iteration just in case it changes in a callback
        for listener in list(self.listeners):
            try:
                getattr(listener, event_function.__name__)(*args, **kwargs)
            except Exception as exception:  # pylint: disable=broad-except
                _LOGGER.error("Listener '%s' produced an exception:\n%s", listener, exception)


class ListenContext:
    """
    A context manager for listening to producer that can generate messages.
    The requirements for the producer are that it has methods:
    * start_listening(..), and,
    * stop_listening(..)
    and that these methods take zero or more arguments that identify the
    listener and perhaps what it wants to listen to if this make sense for
    the producer/listener combination.

    A typical usage would be:
    with ListenContext(producer, listener):
        # Producer generates messages that the listener gets
        pass
    """

    def __init__(self, producer, *args, **kwargs):
        self._producer = producer
        self._args = args
        self._kwargs = kwargs

    def __enter__(self):
        self._producer.add_listener(*self._args, **self._kwargs)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._producer.remove_listener(*self._args, **self._kwargs)


class ThreadSafeCounter:

    def __init__(self):
        self.lock = threading.Lock()
        self.counter = 0

    def increment(self):
        with self.lock:
            self.counter += 1

    def decrement(self):
        with self.lock:
            self.counter -= 1

    @property
    def value(self):
        with self.lock:
            return self.counter


class AttributesFrozendict(frozendict.frozendict):

    def __init__(self, *args, **kwargs):
        super(AttributesFrozendict, self).__init__(*args, **kwargs)
        self._initialised = True

    def __getattr__(self, attr):
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
            errmsg = "'{}' object has no attribute '{}'".format(self.__class__.__name__, attr)
            raise AttributeError(errmsg)

    def __dir__(self):
        """
        So we get tab completion.
        :return: The keys of the dict
        """
        return list(self.keys())


class AttributesDict(types.SimpleNamespace):

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, item):
        try:
            return getattr(self, item)
        except AttributeError:
            raise KeyError("No key '{}'".format(item))

    def __delitem__(self, item):
        return delattr(self, item)

    def setdefault(self, key, value):
        return self.__dict__.setdefault(key, value)

    def get(self, *args, **kwargs):
        return self.__dict__.get(*args, **kwargs)


def load_function(name, instance=None):
    obj = load_object(name)
    if inspect.ismethod(obj):
        if instance is not None:
            return obj.__get__(instance, instance.__class__)

        return obj

    if inspect.isfunction(obj):
        return obj

    raise ValueError("Invalid function name '{}'".format(name))


def load_object(fullname):
    """
    Load a class from a string
    """
    obj, remainder = load_module(fullname)

    # Finally, retrieve the object
    for name in remainder:
        try:
            obj = getattr(obj, name)
        except AttributeError:
            raise ValueError("Could not load object corresponding to '{}'".format(fullname))

    return obj


def load_module(fullname):
    parts = fullname.split('.')

    # Try to find the module, working our way from the back
    mod = None
    remainder = deque()
    for _ in range(len(parts)):
        try:
            mod = importlib.import_module('.'.join(parts))
            break
        except ImportError:
            remainder.appendleft(parts.pop())

    if mod is None:
        raise ValueError("Could not load a module corresponding to '{}'".format(fullname))

    return mod, remainder


def wrap_dict(flat_dict, separator='.'):
    sub_dicts = defaultdict(dict)
    res = {}
    for key, value in flat_dict.items():
        if separator in key:
            namespace, sub_key = key.split(separator, 1)
            sub_dicts[namespace][sub_key] = value
        else:
            res[key] = value
    for namespace, sub_dict in sub_dicts.items():
        res[namespace] = wrap_dict(sub_dict)
    return res


def type_check(obj, expected_type):
    if not isinstance(obj, expected_type):
        raise TypeError("Got object of type '{}' when expecting '{}'".format(type(obj), expected_type))


def ensure_coroutine(wrapped):
    if tornado.gen.is_coroutine_function(wrapped):
        return wrapped

    @tornado.gen.coroutine
    def wrapper(*args, **kwargs):
        raise tornado.gen.Return(wrapped(*args, **kwargs))

    return wrapper


def is_mutable_property(cls, attribute):
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
