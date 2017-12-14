import collections
from enum import Enum
import functools
import inspect
import logging
import os
import six
import sys
import traceback
from utils import call_with_super_check, super_check

__all__ = ['StateMachine', 'event', 'TransitionFailed', 'EventResponse']

_LOGGER = logging.getLogger(__name__)


# Events to be ignored
class EventResponse(Enum):
    IGNORED = 0
    DELAYED = 1


class StateMachineError(BaseException):
    pass


class InvalidStateError(BaseException):
    """The operation is not allowed in this state."""


class EventError(StateMachineError):
    def __init__(self, evt, msg):
        super(EventError, self).__init__(msg)
        self.event = evt


class TransitionFailed(BaseException):
    def __init__(self, initial_state, final_state=None, traceback_str=None):
        self.initial_state = initial_state
        self.final_state = final_state
        self.traceback_str = traceback_str
        super(TransitionFailed, self).__init__(self._format_msg())

    def _format_msg(self):
        msg = ["{} -> {}".format(self.initial_state, self.final_state)]
        if self.traceback_str is not None:
            msg.append(self.traceback_str)
        return "\n".join(msg)


def event(from_states='*', to_states='*'):
    if from_states != '*':
        if inspect.isclass(from_states):
            from_states = (from_states,)
        if not all(issubclass(state, State) for state in from_states):
            raise TypeError()
    if to_states != '*':
        if inspect.isclass(to_states):
            to_states = (to_states,)
        if not all(issubclass(state, State) for state in to_states):
            raise TypeError()

    def wrapper(wrapped):
        evt_label = wrapped.__name__

        @functools.wraps(wrapped)
        def transition(self, *a, **kw):
            initial = self._state

            if from_states != '*' and \
                    not any(isinstance(self._state, state)
                            for state in from_states):
                raise EventError(
                    evt_label,
                    "Event {} invalid in state {}".format(
                        evt_label, initial.LABEL)
                )

            result = wrapped(self, *a, **kw)
            if result not in [EventResponse.IGNORED, EventResponse.DELAYED]:
                if to_states != '*' and not \
                        any(isinstance(self._state, state)
                            for state in to_states):
                    if self._state == initial:
                        raise EventError(evt_label, "Machine did not transition")
                    else:
                        raise EventError(
                            evt_label,
                            "Event produced invalid state transition from "
                            "{} to {}".format(initial.LABEL, self._state.LABEL)
                        )

            return result

        return transition

    if inspect.isfunction(from_states):
        return wrapper(from_states)
    else:
        return wrapper


class State(object):
    LABEL = None
    # A set containing the labels of states that can be entered
    # from this one
    ALLOWED = set()

    in_state = False

    @classmethod
    def is_terminal(cls):
        return not cls.ALLOWED

    def __init__(self, state_machine):
        """
        :param state_machine: The process this state belongs to
        :type state_machine: :class:`StateMachine`
        """
        self.state_machine = state_machine

    def __str__(self):
        return str(self.LABEL)

    @property
    def label(self):
        """ Convenience property to get the state label """
        return self.LABEL

    @super_check
    def enter(self):
        """ Entering the state """
        self.in_state = True

    @super_check
    def exit(self):
        """ Exiting the state """
        if self.is_terminal():
            raise InvalidStateError(
                "Cannot exit a terminal state {}".format(self.LABEL)
            )
        self.in_state = False

    def transition_to(self, state, *args, **kwargs):
        self.state_machine.transition_to(state, *args, **kwargs)


class StateMachine(object):
    STATES = None
    STATES_MAP = None
    sealed = False

    # Class defaults
    _transitioning = False

    @classmethod
    def get_states(cls):
        if cls.STATES is not None:
            return cls.STATES
        else:
            raise RuntimeError("States not defined")

    @classmethod
    def initial_state_label(cls):
        cls.__ensure_built()
        return cls.STATES[0].LABEL

    @classmethod
    def get_state_class(cls, label):
        cls.__ensure_built()
        return cls.STATES_MAP[label]

    @classmethod
    def __ensure_built(cls):
        if cls.sealed:
            return

        cls.STATES = cls.get_states()
        assert isinstance(cls.STATES, collections.Iterable)

        # Build the states map
        cls.STATES_MAP = {}
        for state_cls in cls.STATES:
            assert issubclass(state_cls, State)
            label = state_cls.LABEL
            assert label not in cls.STATES_MAP, \
                "Duplicate label '{}'".format(label)
            cls.STATES_MAP[label] = state_cls

        cls.sealed = True

    def __init__(self, *args, **kwargs):
        self.__ensure_built()
        self._state = None
        self._exception_handler = None
        self.set_debug((not sys.flags.ignore_environment
                        and bool(os.environ.get('PYTHONSMDEBUG'))))
        initial_state = self.get_state_class(self.initial_state_label())
        self.transition_to(initial_state(self, *args, **kwargs))

    def __str__(self):
        return "<{}> ({})".format(self.__class__.__name__, self.state)

    @property
    def state(self):
        return self._state.LABEL

    def on_entered(self):
        """ We've just entered the new state as stored in self._state"""
        pass

    def on_exiting(self):
        """ We're just about the exit the state in self._state"""
        pass

    def on_terminated(self):
        """ Called when a terminal state is entered """
        pass

    def transition_to(self, new_state, *args, **kwargs):
        assert not self._transitioning, \
            "Cannot call transition_to when already transitioning state"
        try:
            initial_state = self._state.LABEL if self._state is not None else None
            self._transitioning = True

            if not isinstance(new_state, State):
                state_cls = self._ensure_state_class(new_state)
                new_state = state_cls(self, *args, **kwargs)
            label = new_state.LABEL

            if self._state is None:
                assert label == self.initial_state_label()
            else:
                assert label in self._state.ALLOWED, \
                    "Cannot transition from {} to {}".format(
                        self._state.LABEL, label)
                self.on_exiting()
                call_with_super_check(self._state.exit)

            call_with_super_check(new_state.enter)
            self._state = new_state

            self.on_entered()
            if self._state.is_terminal():
                self.on_terminated()
        except Exception:
            exc = TransitionFailed(initial_state, new_state, traceback.format_exc())
            self.transition_failed(exc)
        finally:
            self._transitioning = False

    def transition_failed(self, exception):
        """
        Called when a state transitions fails.  This method can be overwritten
        to change the default behaviour which is to raise the exception.

        :param exception: The transition failed exception
        :type exception: :class:`TransitionFailed`
        """
        raise exception

    def get_debug(self):
        return self._debug

    def set_debug(self, enabled):
        self._debug = enabled

    def _ensure_state_class(self, state):
        if inspect.isclass(state) and issubclass(state, State):
            return state
        else:
            try:
                return self.STATES_MAP[state]
            except KeyError:
                raise ValueError("{} is not a valid state".format(state))
