# -*- coding: utf-8 -*-

import logging
import threading
from abc import ABCMeta, abstractmethod

from plum.exceptions import Unsupported, Interrupted
from plum.util import Savable
from plum.util import fullname, protected, override

_LOGGER = logging.getLogger(__name__)


class WaitException(Exception):
    pass


class WaitOn(Savable):
    """
    An object that represents something that is being waited on.

    .. warning:: Only a single thread can `wait` on this wait on.  If it is
        necessary to have another thread wait on the same thing then a copy
        should be made.
    """
    __metaclass__ = ABCMeta

    CLASS_NAME = 'class_name'

    def __str__(self):
        return self.__class__.__name__

    @abstractmethod
    def wait(self, timeout=None):
        """
        Wait for something.

        :param timeout: The waiting timeout
        :return: True if the something happened, False otherwise
        """
        pass

    @abstractmethod
    def interrupt(self):
        pass

    @protected
    def save_instance_state(self, out_state):
        """
        Save the current state of this wait on.  Subclassing methods should
        call the superclass method.

        If a subclassing wait on is unable to save state because, for example,
        it depends on something that is only available at runtime then it
        should raise a :class:`Unsupported` error

        :param out_state: The bundle to save the state into
        """
        out_state[self.CLASS_NAME] = fullname(self)


class WaitEvent(object):
    """
    An event that can be waited upon.
    """

    def __init__(self):
        self._timeout = threading.Condition()
        self._interrupted = False

    def wait(self, timeout=None):
        """
        Wait for the event to happen with an optional timeout.  Waiting can
        be interrupted by calling interrupt() from a different thread in which
        case an Interrupted() exception will be raised.
        
        :param timeout: The wait timeout
        :type timeout: float
        :return: True if the event happened, False otherwise
        """
        with self._timeout:
            self._interrupted = False
            if not self._timeout.wait(timeout):
                return False
            else:
                if self._interrupted:
                    raise Interrupted()
                else:
                    return True

    def interrupt(self):
        """
        If another thread is waiting on this event, interrupt it.  Otherwise
        this call has no effect.
        """
        with self._timeout:
            self._interrupted = True
            self._timeout.notify_all()

    def set(self):
        """
        Set the event as having happened.  If someone was waiting on this event
        they will have True returned from the wait() call.
        """
        with self._timeout:
            self._timeout.notify_all()


class WaitOnEvent(WaitOn):
    def __init__(self):
        super(WaitOnEvent, self).__init__()
        self._init()

    @override
    def load_instance_state(self, saved_state):
        super(WaitOnEvent, self).load_instance_state(saved_state)
        self._init()

    @override
    def wait(self, timeout=None):
        return self._wait_event.wait(timeout)

    @override
    def interrupt(self):
        self._wait_event.interrupt()

    @protected
    def set(self):
        self._wait_event.set()

    def _init(self):
        self._wait_event = WaitEvent()


class WaitOnOneOff(WaitOn):
    """
    Wait on an event that happens once.  After it has happened, subsequent calls
    to wait() will return True immediately.
    """

    def __init__(self):
        super(WaitOnOneOff, self).__init__()
        self._occurred = False
        self._timeout = threading.Event()

    def wait(self, timeout=None):
        if not self._timeout.wait(timeout):
            return False
        else:
            if self._occurred:
                return True
            else:
                self._timeout.clear()
                raise Interrupted()

    def interrupt(self):
        self._timeout.set()

    def save_instance_state(self, out_state):
        super(WaitOnOneOff, self).save_instance_state(out_state)
        out_state['occurred'] = self._occurred

    def load_instance_state(self, saved_state):
        super(WaitOnOneOff, self).load_instance_state(saved_state)
        self._timeout = threading.Event()
        self._occurred = saved_state['occurred']
        if self._occurred:
            self._timeout.set()

    def has_occurred(self):
        return self._occurred

    def set(self):
        """
        Set the event as having occurred.
        """
        self._occurred = True
        self._timeout.set()


def create_from(bundle):
    """
    Load a WaitOn from a save instance state.

    :param bundle: The saved instance state
    :return: The wait on with its state as it was when it was saved
    :rtype: :class:`WaitOn`
    """
    class_name = bundle[WaitOn.CLASS_NAME]
    wait_on_class = bundle.get_class_loader().load_class(class_name)
    return wait_on_class.create_from(bundle)


class Unsavable(object):
    """
    A mixin used to make a wait on unable to be saved or loaded
    """

    @override
    def save_instance_state(self, out_state):
        raise Unsupported("This WaitOn cannot be saved")

    @override
    def load_instance_state(self, saved_state):
        raise Unsupported("This WaitOn cannot be loaded")
