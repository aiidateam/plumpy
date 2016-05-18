# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
from plum.util import fullname, load_class


class WaitOn(object):
    __metaclass__ = ABCMeta

    CLASS_NAME = "class_name"
    CALLBACK_NAME = "callback_name"

    @classmethod
    def create_from(cls, bundle, exec_engine):
        return load_class(bundle[cls.CLASS_NAME]).create_from(bundle, exec_engine)

    def __init__(self, callback_name):
        if not isinstance(callback_name, basestring):
            raise ValueError(
                "callback must be a string corresponding to a method of the Process")
        self._callback = callback_name

    @property
    def callback(self):
        return self._callback

    @abstractmethod
    def is_ready(self):
        pass

    def save_instance_state(self, bundle, exec_engine):
        bundle[self.CLASS_NAME] = fullname(self)
        bundle[self.CALLBACK_NAME] = self.callback
