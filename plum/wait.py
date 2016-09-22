# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
from enum import Enum
from plum.util import fullname, load_class
import inspect


class WaitOn(object):
    __metaclass__ = ABCMeta

    class BundleKeys(Enum):
        CLASS_NAME = "class_name"
        CALLBACK_NAME = "callback_name"

    @classmethod
    def create_from(cls, bundle):
        assert cls.BundleKeys.CLASS_NAME.value in bundle

        class_name = bundle[cls.BundleKeys.CLASS_NAME.value]
        WaitOnClass = bundle.get_class_loader().load_class(class_name)

        return WaitOnClass.create_from(bundle)

    def __init__(self, callback_name):
        self._callback_name = callback_name

    @property
    def callback(self):
        return self._callback_name

    @abstractmethod
    def is_ready(self):
        pass

    def save_instance_state(self, out_state):
        out_state[self.BundleKeys.CLASS_NAME.value] = fullname(self)
        out_state[self.BundleKeys.CALLBACK_NAME.value] = self.callback


class WaitOnError(Exception):
    """
    Exception raised when a wait on cannot determine if it is ready.  This may
    be a permanent or a temporary failure.
    """

    class Nature(Enum):
        """
        Indicates nature of the wait on error.  It can be:
         - UNKNOWN
         - PERMANENT (i.e. it will never fix itself)
         - TEMPORARY (i.e. it will fix itself sometime before the end of the universe)
         - OTHER (e.g. it may be possible to be fixed with user intervention)
        """
        UNKNOWN = 0
        PERMANENT = 1
        TEMPORARY = 2
        OTHER =3

    def __init__(self, msg, nature=Nature.UNKNOWN):
        super(WaitOnError, self).__init__(msg)
        self._nature = nature

    @property
    def nature(self):
        return self._nature


def validate_callback_func(callback):
    from plum.process import Process

    assert inspect.ismethod(callback), "Callback is not a member function"
    assert isinstance(callback.im_self, Process),\
        "Callback must be a method of a Process instance"
    args = inspect.getargspec(callback)[0]
    assert len(args) == 2, \
        "Callback function must take two arguments: self and the wait_on"
