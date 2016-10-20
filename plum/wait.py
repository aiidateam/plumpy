# -*- coding: utf-8 -*-

from abc import ABCMeta
import threading
from plum.util import fullname, protected
from plum.persistence.bundle import Bundle


class WaitOn(object):
    __metaclass__ = ABCMeta

    CLASS_NAME = "class_name"
    OUTCOME = "outcome"

    @staticmethod
    def _is_saved_state(args):
        return len(args) == 1 and isinstance(args[0], Bundle)

    @staticmethod
    def create_from(bundle):
        class_name = bundle[WaitOn.CLASS_NAME]
        WaitOnClass = bundle.get_class_loader().load_class(class_name)
        return WaitOnClass(bundle)

    def __init__(self, *args, **kwargs):
        self._done = threading.Event()
        self._outcome = None
        self._done_callbacks = list()

        if self._is_saved_state(args):
            self.load_instance_state(args[0])
        else:
            self.init(*args, **kwargs)

    def is_done(self):
        """
        Indicate if finished waiting or not.
        To find out if what the outcome is call `get_outcome`

        :return: True if finished, False otherwise.
        """
        return self._outcome is not None

    def get_outcome(self):
        """
        Get the outcome of waiting.  Returns a tuple consisting of (bool, str)
        where the first value indicates success or failure, while the second
        gives an optional message.

        :return: A tuple indicating the outcome of waiting.
        :rtype: tuple
        """
        return self._outcome

    def add_done_callback(self, fn):
        self._done_callbacks.append(fn)
        if self.is_done():
            fn(self)

    def remove_done_callback(self, fn):
        del self._done_callbacks[fn]

    def save_instance_state(self, out_state):
        out_state[self.CLASS_NAME] = fullname(self)
        out_state[self.OUTCOME] = self._outcome

    def wait(self, timeout=None):
        self._done.wait(timeout)

    @protected
    def init(self, *args, **kwargs):
        pass

    @protected
    def load_instance_state(self, bundle):
        self._outcome = bundle[self.OUTCOME]
        if self._outcome is not None:
            self._done.set()

    @protected
    def done(self, success, msg=None):
        """
        Implementing classes should call this when they are done waiting.  As
        well as indicating success or failure they can provide an outcome
        message.

        :param success: True if finished waiting successfully, False otherwise.
        :type success: bool
        :param msg: An (optional) message
        :type msg: str
        """
        assert not self._done.is_set()

        self._outcome = success, msg
        self._done.set()
        for fn in self._done_callbacks:
            fn(self)
