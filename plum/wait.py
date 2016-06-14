# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
from plum.util import fullname, load_class
from plum.process import Process
import inspect


class WaitOn(object):
    __metaclass__ = ABCMeta

    CLASS_NAME = "class_name"
    CALLBACK_NAME = "callback_name"

    @classmethod
    def create_from(cls, bundle, process_factory):
        assert cls.CLASS_NAME in bundle

        return load_class(bundle[cls.CLASS_NAME]).\
            create_from(bundle, process_factory)

    def __init__(self, callback_name):
        self._callback_name = callback_name

    @property
    def callback(self):
        return self._callback_name

    @abstractmethod
    def is_ready(self, registry):
        pass

    def save_instance_state(self, out_state):
        out_state[self.CLASS_NAME] = fullname(self)
        out_state[self.CALLBACK_NAME] = self.callback


def validate_callback_func(callback):
    assert inspect.ismethod(callback), "Callback is not a member function"
    assert isinstance(callback.im_self, Process),\
        "Callback must be a method of a Process instance"
    args = inspect.getargspec(callback)[0]
    assert len(args) == 2, \
        "Callback function must take two arguments: self and the wait_on"
