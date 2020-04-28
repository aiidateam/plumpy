"""The state machine for processes"""

from __future__ import absolute_import

# ABC imports that support python 2 & 3
try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable
import enum
import functools
import inspect
import logging
import os
import sys

import plumpy

from .utils import call_with_super_check, super_check

__all__ = ['StateMachine', 'StateMachineMeta', 'event', 'TransitionFailed']

_LOGGER = logging.getLogger(__name__)


class StateMachineError(Exception):
    """Base class for state machine errors"""


class StateEntryFailed(Exception):
    """
    Failed to enter a state, can provide the next state to go to via this exception
    """

    def __init__(self, state=None, *args, **kwargs):
        super(StateEntryFailed, self).__init__('failed to enter state')
        self.state = state
        self.args = args
        self.kwargs = kwargs


class InvalidStateError(Exception):
    """The operation is not allowed in this state."""


class EventError(StateMachineError):

    def __init__(self, evt, msg):
        super(EventError, self).__init__(msg)
        self.event = evt


class TransitionFailed(Exception):
    """A state transition failed"""

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

            if from_states != '*' and not any(isinstance(self._state, state) for state in from_states):
                raise EventError(evt_label, "Event {} invalid in state {}".format(evt_label, initial.LABEL))

            result = wrapped(self, *a, **kw)
            if not (result is False or isinstance(result, plumpy.Future)):
                if to_states != '*' and not any(isinstance(self._state, state) for state in to_states):
                    if self._state == initial:
                        raise EventError(evt_label, "Machine did not transition")
                    else:
                        raise EventError(
                            evt_label, "Event produced invalid state transition from "
                            "{} to {}".format(initial.LABEL, self._state.LABEL))

            return result

        return transition

    if inspect.isfunction(from_states):
        return wrapper(from_states)

    return wrapper


class State(object):
    LABEL = None
    # A set containing the labels of states that can be entered
    # from this one
    ALLOWED = set()

    @classmethod
    def is_terminal(cls):
        return not cls.ALLOWED

    def __init__(self, state_machine):
        """
        :param state_machine: The process this state belongs to
        :type state_machine: :class:`StateMachine`
        """
        self.state_machine = state_machine
        self.in_state = False

    def __str__(self):
        return str(self.LABEL)

    @property
    def label(self):
        """ Convenience property to get the state label """
        return self.LABEL

    @super_check
    def enter(self):
        """ Entering the state """
        pass

    def execute(self):
        """
        Execute the state, performing the actions that this state is responsible
        for.  Return a state to transition to or None if finished.
        """
        pass

    @super_check
    def exit(self):
        """ Exiting the state """
        if self.is_terminal():
            raise InvalidStateError("Cannot exit a terminal state {}".format(self.LABEL))
        pass

    def create_state(self, state_label, *args, **kwargs):
        return self.state_machine.create_state(state_label, *args, **kwargs)

    def do_enter(self):
        call_with_super_check(self.enter)
        self.in_state = True

    def do_exit(self):
        call_with_super_check(self.exit)
        self.in_state = False


class StateEventHook(enum.Enum):
    """
    Hooks that can be used to register callback at various points in the state transition
    procedure.  The callback will be passed a state instance whose meaning will differ depending
    on the hook as commented below.
    """
    ENTERING_STATE = 0  # State passed will be the state that is being entered
    ENTERED_STATE = 1  # State passed will be the last state that we entered from
    EXITING_STATE = 2  # State passed will be the next state that will be entered (or None for terminal)


class StateMachineMeta(type):

    def __call__(cls, *args, **kwargs):
        """
        Create the state machine and enter the initial state.

        :param args: Any positional arguments to be passed to the constructor
        :param kwargs: Any keyword arguments to be passed to the constructor
        :return: An instance of the state machine
        """
        inst = super(StateMachineMeta, cls).__call__(*args, **kwargs)
        inst.transition_to(inst.create_initial_state())
        call_with_super_check(inst.init)
        return inst


class StateMachine(metaclass=StateMachineMeta):
    STATES = None
    _STATES_MAP = None

    _transitioning = False
    _transition_failing = False

    @classmethod
    def get_states_map(cls):
        cls.__ensure_built()
        return cls._STATES_MAP

    @classmethod
    def get_states(cls):
        if cls.STATES is not None:
            return cls.STATES

        raise RuntimeError("States not defined")

    @classmethod
    def initial_state_label(cls):
        cls.__ensure_built()
        return cls.STATES[0].LABEL

    @classmethod
    def get_state_class(cls, label):
        cls.__ensure_built()
        return cls._STATES_MAP[label]

    @classmethod
    def __ensure_built(cls):
        try:
            # Check if it's already been built (and therefore sealed)
            if cls.__getattribute__(cls, 'sealed'):
                return
        except AttributeError:
            pass

        cls.STATES = cls.get_states()
        assert isinstance(cls.STATES, Iterable)

        # Build the states map
        cls._STATES_MAP = {}
        for state_cls in cls.STATES:
            assert issubclass(state_cls, State)
            label = state_cls.LABEL
            assert label not in cls._STATES_MAP, "Duplicate label '{}'".format(label)
            cls._STATES_MAP[label] = state_cls

        cls.sealed = True

    def __init__(self):
        super(StateMachine, self).__init__()
        self.__ensure_built()
        self._state = None
        self._exception_handler = None
        self.set_debug((not sys.flags.ignore_environment and bool(os.environ.get('PYTHONSMDEBUG'))))
        self._transitioning = False
        self._event_callbacks = {}

    @super_check
    def init(self):
        """ Called after entering initial state. """
        pass

    def __str__(self):
        return "<{}> ({})".format(self.__class__.__name__, self.state)

    def create_initial_state(self):
        return self.get_state_class(self.initial_state_label())(self)

    @property
    def state(self):
        if self._state is None:
            return None
        return self._state.LABEL

    def add_state_event_callback(self, hook, callback):
        """
        Add a callback to be called on a particular state event hook.
        The callback should have form fn(state_machine, hook, state)

        :param hook: The state event hook
        :param callback: The callback function
        """
        self._event_callbacks.setdefault(hook, []).append(callback)

    def remove_state_event_callback(self, hook, callback):
        try:
            self._event_callbacks[hook].remove(callback)
        except (KeyError, ValueError):
            raise ValueError("Callback not set for hook '{}'".format(hook))

    def _fire_state_event(self, hook, state):
        for callback in self._event_callbacks.get(hook, []):
            callback(self, hook, state)

    @super_check
    def on_terminated(self):
        """ Called when a terminal state is entered """
        pass

    def transition_to(self, new_state, *args, **kwargs):
        assert not self._transitioning, \
            "Cannot call transition_to when already transitioning state"

        initial_state_label = self._state.LABEL if self._state is not None else None
        label = None
        try:
            self._transitioning = True

            # Make sure we have a state instance
            new_state = self._create_state_instance(new_state, *args, **kwargs)
            label = new_state.LABEL
            self._exit_current_state(new_state)

            try:
                self._enter_next_state(new_state)
            except StateEntryFailed as exception:
                new_state = exception.state
                # Make sure we have a state instance
                new_state = self._create_state_instance(new_state, *exception.args, **exception.kwargs)
                label = new_state.LABEL
                self._exit_current_state(new_state)
                self._enter_next_state(new_state)

            if self._state.is_terminal():
                call_with_super_check(self.on_terminated)
        except Exception as exc:
            self._transitioning = False
            if self._transition_failing:
                raise
            self._transition_failing = True
            self.transition_failed(initial_state_label, label, *sys.exc_info()[1:])
        finally:
            self._transition_failing = False
            self._transitioning = False

    def transition_failed(self, initial_state, final_state, exception, trace):
        """
        Called when a state transitions fails.  This method can be overwritten
        to change the default behaviour which is to raise the exception.

        :param exception: The transition failed exception
        :type exception: :class:`Exception`
        """
        raise exception.with_traceback(trace)

    def get_debug(self):
        return self._debug

    def set_debug(self, enabled):
        self._debug = enabled

    def create_state(self, state_label, *args, **kwargs):
        try:
            return self.get_states_map()[state_label](self, *args, **kwargs)
        except KeyError:
            raise ValueError("{} is not a valid state".format(state_label))

    def _exit_current_state(self, next_state):
        """ Exit the given state """

        # If we're just being constructed we may not have a state yet to exit,
        # in which case check the new state is the initial state
        if self._state is None:
            if next_state.label != self.initial_state_label():
                raise RuntimeError("Cannot enter state '{}' as the initial state".format(next_state))
            return  # Nothing to exit

        if next_state.LABEL not in self._state.ALLOWED:
            raise RuntimeError("Cannot transition from {} to {}".format(self._state.LABEL, next_state.label))
        self._fire_state_event(StateEventHook.EXITING_STATE, next_state)
        self._state.do_exit()

    def _enter_next_state(self, next_state):
        last_state = self._state
        self._fire_state_event(StateEventHook.ENTERING_STATE, next_state)
        # Enter the new state
        next_state.do_enter()
        self._state = next_state
        self._fire_state_event(StateEventHook.ENTERED_STATE, last_state)

    def _create_state_instance(self, state, *args, **kwargs):
        if isinstance(state, State):
            # It's already a state instance
            return state

        # OK, have to create it
        state_cls = self._ensure_state_class(state)
        return state_cls(self, *args, **kwargs)

    def _ensure_state_class(self, state):
        if inspect.isclass(state) and issubclass(state, State):
            return state

        try:
            return self.get_states_map()[state]
        except KeyError:
            raise ValueError("{} is not a valid state".format(state))
