# -*- coding: utf-8 -*-

from abc import abstractmethod
import frozendict
import importlib
import inspect
import logging
import plum.lang
import threading
from plum.exceptions import ClassNotFoundException, InvalidStateError, CancelledError
from plum.settings import check_protected, check_override

__all__ = ['loop_factory', 'get_default_loop']

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


class Future(object):
    """
    A generic future object.  Can be used as is or subclassed.
    """
    _UNSET = ()

    def __init__(self):
        self._state = _PENDING
        self._result = self._UNSET
        self._exception = None

    def cancel(self):
        if self.done():
            return False

        self._state = _CANCELLED
        return True

    def cancelled(self):
        return self._state is _CANCELLED

    def done(self):
        return self._state != _PENDING

    def result(self):
        if self.cancelled():
            raise CancelledError()
        elif self._state is not _FINISHED:
            raise InvalidStateError("The future has not completed yet")
        elif self._exception is not None:
            raise self._exception

        return self._result

    def set_result(self, result):
        if self.done():
            raise InvalidStateError("The future is already done")

        self._result = result
        self._state = _FINISHED

    def exception(self):
        if self.cancelled():
            raise CancelledError()
        if self._state is not _FINISHED:
            raise InvalidStateError("Exception not set")

        return self._exception

    def set_exception(self, exception):
        if self.done():
            raise InvalidStateError("The future is already done")

        self._exception = exception
        self._state = _FINISHED


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


class Savable(object):
    @classmethod
    def create_from(cls, saved_state):
        """
        Create the wait on from a save instance state.

        :param saved_state: The saved instance state
        :type saved_state: :class:`plum.persistence.Bundle`
        :return: The wait on with its state as it was when it was saved
        """
        obj = cls.__new__(cls)
        obj.load_instance_state(saved_state)
        return obj

    @abstractmethod
    def save_instance_state(self, out_state):
        pass

    @abstractmethod
    def load_instance_state(self, saved_state):
        pass


class SavableWithClassloader(Savable):
    def save_instance_state(self, out_state):
        pass


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


def process_factory(loop, task_class, *args, **kwargs):
    from plum.persistence import Bundle

    if args and len(args) == 1 and isinstance(args[0], Bundle):
        if kwargs:
            RuntimeError("Found unexpected kwargs in call to process factory")
        return task_class.create_from(loop, args[0])
    elif kwargs and 'saved_state' in kwargs:
        return task_class.create_from(loop, kwargs['saved_state'])
    else:
        return task_class(loop, *args, **kwargs)


def loop_factory(*args, **kwargs):
    from plum.loop import BaseEventLoop

    loop = BaseEventLoop(*args, **kwargs)
    loop.set_task_factory(process_factory)
    return loop


def get_default_loop():
    global _default_loop
    if _default_loop is None:
        _default_loop = loop_factory()

    return _default_loop
