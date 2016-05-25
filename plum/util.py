# -*- coding: utf-8 -*-

import threading
import importlib
import frozendict
from enum import Enum


class ProcessState(Enum):
    RUNNING = 0
    WAITING = 1


class EventHelper(object):
    def __init__(self, listener_type):
        assert(listener_type is not None)
        self._listener_type = listener_type
        self._listeners = []

    def add_listener(self, listener):
        assert(isinstance(listener, self._listener_type))
        self._listeners.append(listener)

    def remove_listener(self, listener):
        self._listeners.remove(listener)

    @property
    def listeners(self):
        return self._listeners

    def fire_event(self, event_function, *args, **kwargs):
        # TODO: Check if the function is in the listener type
        # We have to use a copy here because the listener may
        # remove themselves during the message
        for l in list(self.listeners):
            getattr(l, event_function)(*args, **kwargs)


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


def fullname(object):
    """
    Get the fully qualified name of an object.

    :param object: The object to get the name from.
    :return: The fully qualified name.
    """
    return object.__module__ + "." + object.__class__.__name__


def load_class(classstring):
    """
    Load a class from a string
    """
    class_data = classstring.split(".")
    module_path = ".".join(class_data[:-1])
    class_name = class_data[-1]

    module = importlib.import_module(module_path)
    # Finally, retrieve the Class
    return getattr(module, class_name)


class AttributesFrozendict(frozendict.frozendict):
    def __getattr__(self, attr):
        """
        Read a key as an attribute. Raise AttributeError on missing key.
        Called only for attributes that do not exist.
        """
        try:
            return self[attr]
        except KeyError:
            errmsg = "'{}' object has no attribute '{}'".format(
                self.__class__.__name__, attr)
            raise AttributeError(errmsg)
