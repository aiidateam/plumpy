# -*- coding: utf-8 -*-

import abc
from apricotpy import persistable
import frozendict
import importlib
import inspect
import logging
import plum.lang
import threading
from plum.exceptions import ClassNotFoundException, InvalidStateError, CancelledError
from plum.settings import check_protected, check_override

__all__ = ['loop_factory']

protected = plum.lang.protected(check=check_protected)
override = plum.lang.override(check=check_override)

_LOGGER = logging.getLogger(__name__)
_default_loop = None


class EventHelper(object):
    def __init__(self, listener_type, raise_exceptions=False):
        assert (listener_type is not None), "Must provide valid listener type"

        self._listener_type = listener_type
        self._raise_exceptions = raise_exceptions
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
        assert event_function is not None, "Must provide valid event method"

        # TODO: Check if the function is in the listener type
        # We have to use a copy here because the listener may
        # remove themselves during the message
        for l in list(self.listeners):
            try:
                getattr(l, event_function.__name__)(*args, **kwargs)
            except BaseException as e:
                import traceback
                traceback.print_exc()

                _LOGGER.error(
                    "The listener '{}' produced an exception while receiving "
                    "the message '{}':\n{}: {}".format(
                        l, event_function.__name__, e.__class__.__name__, e.message)
                )
                if self._raise_exceptions:
                    raise


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


def fullname(object):
    """
    Get the fully qualified name of an object.

    :param object: The object to get the name from.
    :return: The fully qualified name.
    """
    if inspect.isclass(object):
        return object.__module__ + "." + object.__name__
    else:
        return object.__module__ + "." + object.__class__.__name__


def load_class(classstring):
    """
    Load a class from a string
    """
    class_data = classstring.split(".")
    module_path = ".".join(class_data[:-1])
    class_name = class_data[-1]

    module = importlib.import_module(module_path)

    # Finally, retrieve the class
    try:
        return getattr(module, class_name)
    except AttributeError:
        raise ClassNotFoundException("Class {} not found".format(classstring))


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


def load_with_classloader(bundle):
    """
    Load a process from a saved instance state

    :param bundle: The saved instance state bundle
    :return: The process instance
    :rtype: :class:`Process`
    """
    # Get the class using the class loader and instantiate it
    class_name = bundle['class_name']
    proc_class = bundle.get_class_loader().load_class(class_name)
    return proc_class.create_from(bundle)


def loop_factory(*args, **kwargs):
    loop = persistable.BaseEventLoop()
    return loop


def set_if_not_none(mapping, key, value):
    """
    Set the given value in a mapping only if the value is not `None`,
    otherwise the mapping is left untouched
    
    :param mapping: The mapping to set the value for 
    :param key: The mapping key
    :param value: The mapping value
    """
    if value is not None:
        mapping[key] = value
