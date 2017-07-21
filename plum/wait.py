# -*- coding: utf-8 -*-

import logging
import threading
from abc import ABCMeta, abstractmethod

import plum.loop.persistence
from plum.exceptions import Unsupported, Interrupted
from plum.loop import objects, persistence
from plum.util import fullname, protected, override

_LOGGER = logging.getLogger(__name__)


class WaitException(Exception):
    pass


class WaitOn(plum.loop.persistence.PersistableAwaitable,
             plum.loop.persistence.PersistableLoopObject):
    """
    An object that represents something that is being waited on.
    """
    __metaclass__ = ABCMeta

    CLASS_NAME = 'class_name'

    def __str__(self):
        return self.__class__.__name__


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
