# -*- coding: utf-8 -*-

from abc import ABCMeta
from enum import Enum

from plum.process import Process
from plum.wait import WaitOn
from plum.persistence.bundle import Bundle
import plum.util as util
from plum.util import protected, override
import threading


class WaitingProcess(Process):
    __metaclass__ = ABCMeta

    # String keys used by the process to save its state in the state bundle.
    # See create_from, on_save_instance_state and _load_instance_state.
    WAITING_ON = 'waiting_on'

    @staticmethod
    def _is_wait_retval(retval):
        return (isinstance(retval, tuple) and
                len(retval) == 2 and
                isinstance(retval[0], WaitOn))

    @classmethod
    def create_waiting_on(cls, saved_instance_state):
        return WaitOn.create_from(
            saved_instance_state[cls.WAITING_ON])

    def __init__(self):
        super(WaitingProcess, self).__init__()
        self._waiter = threading.Event()
        self._waiting_on = None
        self.__interrupted = False

        # Events and running
        self.__event_helper = util.EventHelper(WaitListener)

    def add_wait_listener(self, l):
        self.__event_helper.add_listener(l)

    def remove_wait_listener(self, l):
        self.__event_helper.remove_listener(l)

    @override
    def do_run(self):
        retval = super(WaitingProcess, self).do_run()

        while self._is_wait_retval(retval):
            self._perform_wait(wait_on=retval[0], next_step=retval[1])
            if self.__interrupted:
                return None
            else:
                retval = self._perform_continue()

        return retval

    def is_waiting(self):
        return self._waiting_on is not None

    @protected
    def on_wait(self, wait_on):
        """
        Message received when the process is about to start waiting on a WaitOn
        to be ready.

        :param wait_on: The WaitOn being waited on
        :type wait_on: :class:`WaitOn`
        """
        self._waiting_on = wait_on
        self.__event_helper.fire_event(
            WaitListener.on_process_wait, self, wait_on)
        self._called = True

    @protected
    def on_continue(self, wait_on):
        """
        Message received when the process has finished waiting.  The wait on
        may, or may not, have been successful.  It's up to the user to check.

        :param wait_on: The wait on that has finished
        :type wait_on: :class:`WaitOn`
        """
        self.__event_helper.fire_event(
            WaitListener.on_process_continue, self)
        self._called = True

    @override
    def interrupt(self):
        print("Interrupting")
        self.__interrupted = True
        self._waiter.set()

    @override
    def save_instance_state(self, bundle):
        super(WaitingProcess, self).save_instance_state(bundle)

        wait_on_state = None
        if self._waiting_on is not None:
            wait_on_state = Bundle()
            self._waiting_on.save_instance_state(wait_on_state)
        bundle[self.WAITING_ON] = wait_on_state

    @override
    def load_instance_state(self, bundle):
        super(WaitingProcess, self).load_instance_state(bundle)

        if bundle[self.WAITING_ON]:
            self._waiting_on = \
                WaitOn.create_from(bundle[self.WAITING_ON])

    def _perform_wait(self, wait_on, next_step):
        """
        Messages issued:
         - on_wait
        """
        self._waiting_on = wait_on
        self._next_step = next_step

        self._called = False
        self.on_wait(wait_on)
        assert self._called, \
            "on_wait was not called\n" \
            "Hint: Did you forget to call the superclass method?"

        self._waiter.clear()
        self._waiting_on.add_done_callback(self._done_waiting)
        self._waiter.wait()

    def _perform_continue(self):
        """
        Messages issued:
         - on_continue
        """
        wait_on, next_step = self._waiting_on, self._next_step
        self._waiting_on = None
        self._next_step = None

        self._called = False
        self.on_continue(wait_on)
        assert self._called, \
            "on_continue was not called\n" \
            "Hint: Did you forget to call the superclass method?"

        fn = self.__getattribute__(next_step)
        return fn(wait_on)

    def _done_waiting(self, wait_on):
        self._waiter.set()


class WaitListener(object):
    __metaclass__ = ABCMeta

    def on_process_wait(self, process, wait_on):
        pass

    def on_process_continue(self, process):
        pass
