import collections
from utils import call_with_super_check, super_check

# Events to be ignored
IGNORE_EVENT = 0


class InvalidStateError(BaseException):
    """The operation is not allowed in this state."""


class State(object):
    LABEL = None
    # A set containing the labels of states that can be entered
    # from this one
    ALLOWED = set()
    # A map of [Event] : [State label] representing transitions
    # that are automatically performed in response to the event.
    # WARNING: You have to call super() in your evt() method for
    # this to be picked up.
    TRANSITIONS = {}

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
        self.in_state = True

    @super_check
    def exit(self):
        """ Exiting the state """
        if self.is_terminal():
            raise InvalidStateError(
                "Cannot exit a terminal state {}".format(self.LABEL)
            )
        self.in_state = False

    def evt(self, event, *args, **kwargs):
        """
        An event has occurred

        :param event: The event label
        :param args: Optional event arguments
        :param kwargs: Optional event keyword arguments
        """
        new_state = self.TRANSITIONS[event]
        if new_state != IGNORE_EVENT:
            self.transition_to(new_state, *args, **kwargs)

    def transition_to(self, state, *args, **kwargs):
        self.state_machine._transition_to(state, *args, **kwargs)


class StateMachine(object):
    STATES = None
    STATES_MAP = None
    sealed = False

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
        initial_state = self.get_state_class(self.initial_state_label())
        self._transition_to(initial_state(self, *args, **kwargs))

    @property
    def state(self):
        return self._state.LABEL

    def on_entered(self):
        """ We've just entered the new state as stored in self._state"""
        pass

    def on_exiting(self):
        """ We're just about the exit the state in self._state"""
        pass

    def evt(self, event, *args, **kwargs):
        self._state.evt(event, *args, **kwargs)

    def _transition_to(self, new_state, *args, **kwargs):
        if isinstance(new_state, State):
            label = new_state.LABEL
        else:
            # Assume that 'state' is a label and create
            label = new_state
            new_state = self.get_state_class(label)(self, *args, **kwargs)

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
