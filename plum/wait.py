# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
from plum.util import fullname, load_class

class WaitOn(object):
    __metaclass__ = ABCMeta

    CLASS_NAME = "class_name"
    CALLBACK_NAME = "callback_name"

    @classmethod
    def create_from(cls, bundle, process_manager):
        assert cls.CLASS_NAME in bundle

        return load_class(bundle[cls.CLASS_NAME]).\
            create_from(bundle, process_manager)

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
