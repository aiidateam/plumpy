# -*- coding: utf-8 -*-

import threading
from abc import ABCMeta
import logging

from plum.persistence.bundle import Bundle
from plum.util import fullname, protected, override
from plum.exceptions import Unsupported
from plum.util import Savable

_LOGGER = logging.getLogger(__name__)


class Interrupted(Exception):
    pass


class WaitOn(Savable):
    """
    An object that represents something that is being waited on.

    .. warning:: Only a single thread can `wait` on this wait on.  If it is
        necessary to have another thread wait on the same thing then a copy
        should be made.
    """
    __metaclass__ = ABCMeta

    CLASS_NAME = "class_name"
    OUTCOME = "outcome"
    RECREATE_FROM_KEY = 'recreate_from'

    @staticmethod
    def _is_saved_state(args):
        return len(args) == 1 and isinstance(args[0], Bundle)

    def __init__(self, *args, **kwargs):
        self._init()

    def __str__(self):
        return self.__class__.__name__

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
        # TODO: Add check that this is not called from multiple threads simultaneously
        with self._interrupt_lock:
            if self.is_done():
                return True
                # Going to have to wait

        if not self._waiting.wait(timeout):
            # The threading Event returns False if it timed out
            _LOGGER.debug("Wait on '{}' timed out".format(self.__class__.__name__))
            return False
        elif self.is_done():
            _LOGGER.debug("Wait on '{}' finished".format(self.__class__.__name__))
            return True
        else:
            # Must have been interrupted
            _LOGGER.debug("Wait on '{}' interrupted".format(self.__class__.__name__))
            raise Interrupted()

    def interrupt(self):
        with self._interrupt_lock:
            self._waiting.set()

    def clear(self):
        """
        Clear the wait on, including any outstanding interrupt requests
        """
        self._waiting.clear()

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
        out_state[self.OUTCOME] = self._outcome

    @protected
    def load_instance_state(self, saved_state):
        """
        Load the state of a wait on from a saved instance state.  All
        subclasses implementing this should call the superclass method.

        :param saved_state: :class:`Bundle` The save instance state
        """
        self._init()

        outcome = saved_state[self.OUTCOME]
        if outcome is not None:
            self.done(outcome[0], outcome[1])

    @protected
    def done(self, success=True, msg=None):
        """
        Implementing classes should call this when they are done waiting.  As
        well as indicating success or failure they can provide an optional
        outcome message.

        :param success: True if finished waiting successfully, False otherwise.
        :type success: bool
        :param msg: An (optional) message
        :type msg: str
        """
        assert self._outcome is None, "Cannot call done more than once"

        with self._interrupt_lock:
            self._outcome = success, msg
            self._waiting.set()

    def _init(self):
        self._outcome = None

        # Variables below this don't need to be saved in the instance state
        self._waiting = threading.Event()
        self._interrupt_lock = threading.Lock()
        self.__super_called = False


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
    def load_instance_state(self, bundle):
        raise Unsupported("This WaitOn cannot be loaded")
