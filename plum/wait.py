# -*- coding: utf-8 -*-

from abc import ABCMeta
import threading
from plum.util import fullname, protected
from plum.persistence.bundle import Bundle


class Interrupted(Exception):
    pass


class WaitOn(object):
    """
    An object that represents something that is being waited on.

    .. warning:: This object has the following behaviour if used by multiple
        threads.  If two threads call wait() and then a call to interrupt() is
        received then they are both interrupted.  If this is not the desired
        behaviour (i.e. interruption being per thread) then make a copy in each
        thread.
    """
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
        self._outcome = None

        # Variables below this don't need to be saved in the instance state
        self._done = threading.Event()
        self._interrupted = False

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

    def save_instance_state(self, out_state):
        out_state[self.CLASS_NAME] = fullname(self)
        out_state[self.OUTCOME] = self._outcome

    def wait(self, timeout=None):
        """
        Block until this wait on to completes.  If a timeout is supplied it is
        interpreted to be a float in second (or fractions thereof).  If the
        timeout is reached without the wait on being done this method will
        return False.

        :param timeout: An optional timeout after which this method will
            return with the value False.
        :type timeout: float
        :raise: :class:`Interrupted` if :func:`interrupt` is called before the
            wait on is done
        :return: True if the wait on has completed, False otherwise.
        """
        # The threading Event returns False if it timed out
        if not self._done.wait(timeout):
            return False
        if self._interrupted:
            self._interrupted = False
            raise Interrupted()

        return True

    def interrupt(self):
        threading.local()._interrupted = True
        self._done.set()

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
