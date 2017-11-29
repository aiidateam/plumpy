import abc
from enum import Enum


class CancelledError(BaseException):
    """The process was cancelled."""


class InvalidStateError(BaseException):
    """The operation is not allowed in this state."""


# region Commands
class Command(object):
    pass


class Cancel(Command):
    def __init__(self, msg=None):
        self.msg = msg


class Wait(Command):
    def __init__(self, continue_fn=None, desc=None):
        self.continue_fn = continue_fn
        self.desc = desc


class Stop(Command):
    def __init__(self, result):
        self.result = result


class Continue(Command):
    def __init__(self, continue_fn, *args, **kwargs):
        self.continue_fn = continue_fn
        self.args = args
        self.kwargs = kwargs


# endregion


# region States

class ProcessState(Enum):
    """
    The possible states that a :class:`Process` can be in.
    """
    CREATED = 0
    RUNNING = 1
    WAITING = 2
    PAUSED = 3
    STOPPED = 4
    FAILED = 5
    CANCELLED = 6


class State(object):
    __metaclass__ = abc.ABCMeta
    STATE = None

    @classmethod
    def is_terminal(cls):
        return not cls.ALLOWED

    def __init__(self, process):
        """
        :param process: The process this state belongs to
        :type process: :class:`Process`
        """
        self.process = process

    def __str__(self):
        return str(self.STATE)

    @abc.abstractmethod
    def enter(self):
        pass

    def exit(self):
        if self.is_terminal():
            raise InvalidStateError(
                "Cannot exit a terminal state {}".format(self.STATE)
            )

    def save_instance_state(self, out_state):
        pass

    def load_instance_state(self, process, saved_state):
        self.process = process

    def cancel(self, msg):
        self.process._transition(Cancelled(self.process, msg))


class Created(State):
    STATE = ProcessState.CREATED
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.CANCELLED,
               ProcessState.FAILED}

    def enter(self):
        self.process.on_created()

    def exit(self):
        self.process.on_exit_created()

    def play(self):
        self.process._transition(Running(self.process, self.process.run))

    def pause(self):
        # Ignored
        pass


class Running(State):
    STATE = ProcessState.RUNNING
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.WAITING,
               ProcessState.PAUSED,
               ProcessState.STOPPED,
               ProcessState.CANCELLED,
               ProcessState.FAILED}

    RUN_FN = 'RUN_FN'
    ARGS = 'ARGS'
    KWARGS = 'KWARGS'
    PAUSING = 'PAUSING'
    CANCELLING_MSG = 'CANCELLING_MSG'

    def __init__(self, process, run_fn, *args, **kwargs):
        super(Running, self).__init__(process)
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs
        self.pausing = False
        self.cancelling = None

    def __str__(self):
        state = str(self.STATE)
        if self.cancelling:
            state += " (cancelling)"
        elif self.pause:
            state += " (pausing)"
        return state

    def enter(self):
        self.process.on_running()

    def do_run(self):
        next_state = None
        try:
            command = self.run_fn(*self.args, **self.kwargs)

            # Cancelling takes precedence over everything else
            if self.cancelling:
                command = self.cancelling
            elif not isinstance(command, Command):
                command = Stop(command)

            if isinstance(command, Cancel):
                next_state = Cancelled(self.process, command.msg)
            else:
                if isinstance(command, Stop):
                    next_state = Stopped(self.process, command.result)
                elif isinstance(command, Wait):
                    next_state = Waiting(
                        self.process, command.continue_fn, command.desc
                    )
                elif isinstance(command, Continue):
                    next_state = Running(
                        self.process, command.continue_fn, *command.args
                    )

                if self.pausing:
                    next_state = Paused(self.process, next_state)

        except BaseException as e:
            next_state = Failed(self.process, e)

        self.process._transition(next_state)

    def exit(self):
        self.process.on_exit_running()

    def save_instance_state(self, out_state):
        super(Running, self).save_instance_state(out_state)
        out_state[self.RUN_FN] = self.run_fn.__name__
        out_state[self.ARGS] = self.args
        out_state[self.KWARGS] = self.kwargs
        out_state[self.PAUSING] = self.pausing
        if self.cancelling:
            out_state[self.CANCELLING_MSG] = self.cancelling.msg

    def load_instance_state(self, process, saved_state):
        super(Running, self).load_instance_state(process, saved_state)
        self.run_fn = getattr(self.process, saved_state[self.RUN_FN])
        self.args = saved_state[self.ARGS]
        self.kwargs = saved_state[self.KWARGS]
        self.pausing = saved_state[self.PAUSING]
        try:
            self.cancelling = Cancel(saved_state[self.CANCELLING_MSG])
        except KeyError:
            self.cancelling = None

    def pause(self):
        self.pausing = True

    def cancel(self, msg):
        self.cancelling = Cancel(msg)


class Waiting(State):
    STATE = ProcessState.WAITING
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.PAUSED,
               ProcessState.CANCELLED,
               ProcessState.FAILED}

    CONTINUE_FN = 'CONTINUE_FN'
    DESC = 'DESC'

    def __init__(self, process, continue_fn, desc):
        super(Waiting, self).__init__(process)
        self.continue_fn = continue_fn
        self.desc = desc

    def __str__(self):
        state = str(self.STATE)
        if self.desc is not None:
            state += " ({})".format(self.desc)
        return state

    def enter(self):
        self.process.on_waiting(self.desc)

    def exit(self):
        self.process.on_exit_waiting()

    def save_instance_state(self, out_state):
        super(Waiting, self).save_instance_state(out_state)
        out_state[self.CONTINUE_FN] = self.continue_fn.__name__
        out_state[self.DESC] = self.desc

    def load_instance_state(self, process, saved_state):
        super(Waiting, self).load_instance_state(process, saved_state)
        self.continue_fn = getattr(process, saved_state[self.CONTINUE_FN])
        self.desc = saved_state[self.DESC]

    def pause(self):
        self.process._transition(Paused(self.process, self))

    def done(self, value):
        if self.continue_fn is None:
            next_state = Stopped(self.process, value)
        else:
            next_state = Running(self.process, self.continue_fn, value)
        self.process._transition(next_state)


class Paused(State):
    STATE = ProcessState.PAUSED
    ALLOWED = {ProcessState.WAITING,
               ProcessState.RUNNING,
               ProcessState.CANCELLED,
               ProcessState.FAILED}

    NEXT_STATE = 'NEXT_STATE'

    def __init__(self, process, next_state):
        """
        :param process: The process this state belongs to
        :param next_state: The state that was interrupted when pausing
        :type next_state: :class:`State`
        """
        super(Paused, self).__init__(process)
        self.next_state = next_state

    def enter(self):
        self.process.on_paused()

    def exit(self):
        self.process.on_exit_paused()

    def save_instance_state(self, out_state):
        super(Paused, self).save_instance_state(out_state)
        next_state_state = {}
        self.next_state.save_instance_state(next_state_state)
        out_state[self.NEXT_STATE] = next_state_state

    def load_instance_state(self, process, saved_state):
        super(Paused, self).load_instance_state(process, saved_state)
        self.next_state = State.create_from(process, saved_state[self.NEXT_STATE])


class Failed(State):
    STATE = ProcessState.FAILED
    ALLOWED = {}  # terminal

    def __init__(self, parent, exception):
        super(Failed, self).__init__(parent)
        self.exception = exception

    def __str__(self):
        exc = str(type(self.exception))
        try:
            exc += " - {}".format(self.exception.message)
        except AttributeError:
            pass
        return "{} ({})".format(self.STATE, exc)

    def enter(self):
        self.process.on_failed(self.exception)


class Stopped(State):
    STATE = ProcessState.FAILED
    ALLOWED = {}  # terminal

    RESULT = 'RESULT'

    def __init__(self, process, result):
        super(Stopped, self).__init__(process)
        self.result = result

    def enter(self):
        self.process.on_stopped(self.result)

    def save_instance_state(self, out_state):
        super(Stopped, self).save_instance_state(out_state)
        out_state[self.RESULT] = self.result

    def load_instance_state(self, process, saved_state):
        super(Stopped, self).load_instance_state(process, saved_state)
        self.result = saved_state[self.RESULT]


class Cancelled(State):
    STATE = ProcessState.CANCELLED
    ALLOWED = {}  # terminal

    MSG = 'MSG'

    def __init__(self, process, msg):
        super(Cancelled, self).__init__(process)
        self.msg = msg

    def enter(self):
        self.process.on_cancelled(self.msg)

    def save_instance_state(self, out_state):
        super(Cancelled, self).save_instance_state(out_state)
        out_state[self.MSG] = self.msg

    def load_instance_state(self, process, saved_state):
        super(Cancelled, self).load_instance_state(process, saved_state)
        self.msg = saved_state[self.MSG]


STATE_CLASSES = {
    ProcessState.CREATED: Created,
    ProcessState.RUNNING: Running,
    ProcessState.WAITING: Waiting,
    ProcessState.STOPPED: Stopped,
    ProcessState.PAUSED: Paused,
    ProcessState.CANCELLED: Cancelled,
    ProcessState.FAILED: Failed
}


class ProcessStateGraph(object):
    # The allowed state transitions
    ALLOWED = {state: state_class.ALLOWED for
               state, state_class in STATE_CLASSES.iteritems()}

    @staticmethod
    def is_allowed(start_state, end_state):
        return end_state in ProcessStateGraph.ALLOWED[start_state]

    @staticmethod
    def is_reachable(start_state, end_state):
        if ProcessStateGraph.is_allowed(start_state, end_state):
            return True
        # From each of the states we can transition to from the start state
        # is there one from which we can reach the end state?
        for state in ProcessStateGraph.ALLOWED[start_state]:
            if ProcessStateGraph.is_reachable(state, end_state):
                return True
        return False

    @staticmethod
    def is_terminal(state):
        return not ProcessStateGraph.ALLOWED[state]

# endregion


class Process(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._state = None
        # Enter the created state
        self._transition(Created(self))

    def __str__(self):
        return "Process {}".format(self._state)

    @property
    def state(self):
        return self._state.STATE

    @abc.abstractmethod
    def run(self):
        pass

    # region State entry/exit events
    def on_created(self):
        pass

    def on_exit_created(self):
        pass

    def on_running(self):
        pass

    def on_exit_running(self):
        pass

    def on_waiting(self, what):
        pass

    def on_exit_waiting(self):
        pass

    def on_paused(self):
        pass

    def on_exit_paused(self):
        pass

    def on_stopped(self, result):
        pass

    def on_failed(self, exception):
        pass

    def on_cancelled(self, msg):
        pass

    # endregion

    def save_instance_state(self, out_state):
        self._state.save_instance_state(out_state)

    def load_instance_state(self, saved_state):
        pass

    def result(self):
        """
        Get the result from the process if it is stopped.
        If the process is CANCELLED then a CancelledError will be raise.
        If in any other state this will raise an InvalidStateError.
        :return: The stop value of the process
        """
        if isinstance(self._state, Stopped):
            return self._state.result
        elif isinstance(self._state, Cancelled):
            raise CancelledError()
        elif isinstance(self._state, Failed):
            raise self._state.exception
        else:
            raise InvalidStateError

    # region External events
    def pause(self):
        """ Pause the process """
        try:
            self._state.pause()
        except AttributeError:
            raise InvalidStateError("Cannot pause in state {}".format(self.state))

    def play(self):
        """
        Play the process if in the CREATED or PAUSED state
        """
        try:
            self._state.play()
        except AttributeError:
            raise InvalidStateError("Cannot play in state {}".format(self.state))

    def cancel(self, msg=None):
        """
        Cancel the process
        :param msg: An optional cancellation message
        :type msg: bool
        """
        try:
            self._state.cancel(msg)
        except AttributeError:
            raise InvalidStateError("Cannot cancel in state {}".format(self.state))

    def resume(self, value=None):
        """
        Resume the process from the waiting state
        :param value: An optional value to pass to either the function that is
            called upon continuing or the final results of the process.
        """
        try:
            self._state.done(value)
        except AttributeError:
            raise InvalidStateError(
                "Cannot finish waiting in state {}".format(self.state)
            )

    # endregion

    def _transition(self, new_state):
        if self._state is not None:
            if new_state.STATE not in self._state.ALLOWED:
                raise RuntimeError("Cannot transition from {} to {}".format(
                    self._state.STATE, new_state.STATE
                ))
            self._state.exit()
        try:
            new_state.enter()
            self._state = new_state
            if isinstance(self._state, Running):
                self._state.do_run()
        except BaseException as e:
            new_state = Failed(self, e)
            # The user is expected to deal with exceptions from entering Failed
            new_state.enter()
            self._state = new_state
