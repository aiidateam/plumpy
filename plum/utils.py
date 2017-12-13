# -*- coding: utf-8 -*-

import importlib
import inspect
import logging
import threading
from collections import deque

import frozendict

import plum.lang
from plum.settings import check_protected, check_override

__all__ = []

protected = plum.lang.protected(check=check_protected)
override = plum.lang.override(check=check_override)

_LOGGER = logging.getLogger(__name__)
_default_loop = None


class EventHelper(object):
    def __init__(self, listener_type, loop):
        assert (listener_type is not None), "Must provide valid listener type"

        self._listener_type = listener_type
        self._loop = loop
        self._listeners = set()

    def add_listener(self, listener):
        assert isinstance(listener, self._listener_type), "Listener is not of right type"
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
            raise ValueError("Must provide valid event method")

        for l in self.listeners:
            try:
                getattr(l, event_function.__name__)(*args, **kwargs)
            except Exception as e:
                _LOGGER.error(
                    "Listener {} produced an exception:\n".format(l, e))


class ListenContext(object):
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


class ThreadSafeCounter(object):
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


_PENDING = 'PENDING'
_CANCELLED = 'CANCELLED'
_FINISHED = 'FINISHED'


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
        if attr == "__setstate__":
            raise AttributeError()
        try:
            return self[attr]
        except KeyError:
            errmsg = "'{}' object has no attribute '{}'".format(
                self.__class__.__name__, attr)
            raise AttributeError(errmsg)

    def __dir__(self):
        """
        So we get tab completion.
        :return: The keys of the dict
        """
        return self.keys()


class SimpleNamespace(object):
    """
    An attempt to emulate python 3's types.SimpleNamespace
    """

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        keys = sorted(self.__dict__)
        items = ("{}={!r}".format(k, self.__dict__[k]) for k in keys)
        return "{}({})".format(type(self).__name__, ", ".join(items))

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


def function_name(fn):
    try:
        name = fn.__module__ + '.' + fn.__qualname__
    except AttributeError:
        if inspect.ismethod(fn):
            cls = fn.__self__.__class__
            name = class_name(cls) + '.' + fn.__name__
        elif inspect.isfunction(fn):
            name = fn.__module__ + '.' + fn.__name__
        else:
            raise ValueError("Must be function or method")

    # Make sure we can load it
    try:
        load_object(name)
    except ValueError:
        raise ValueError("Could not create a consistent name for fn '{}'".format(fn))

    return name


def load_function(name, instance=None):
    obj = load_object(name)
    if inspect.ismethod(obj):
        if instance is not None:
            return obj.__get__(instance, instance.__class__)
        else:
            return obj
    elif inspect.isfunction(obj):
        return obj
    else:
        raise ValueError("Invalid function name '{}'".format(name))


def class_name(obj, class_loader=None):
    """
    Given a class or an instance this function will give the fully qualified name
    e.g. 'my_module.MyClass'

    :param obj: The object to get the name from.
    :return: The fully qualified name.
    """

    if not inspect.isclass(obj):
        # assume it's an instance
        obj = obj.__class__

    name = obj.__module__ + '.' + obj.__name__

    try:
        if class_loader is not None:
            class_loader.load_class(name)
        else:
            load_object(name)
    except ValueError:
        raise ValueError("Could not create a consistent full name for object '{}'".format(obj))

    return name


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
    for i in range(len(parts)):
        try:
            mod = importlib.import_module('.'.join(parts))
            break
        except ImportError:
            remainder.appendleft(parts.pop())

    if mod is None:
        raise ValueError("Could not load a module corresponding to '{}'".format(fullname))

    return mod, remainder


