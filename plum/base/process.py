import abc
from enum import Enum
from . import statemachine
from .statemachine import InvalidStateError
from .utils import super_check, call_with_super_check

__all__ = ['ProcessStateMachine', 'ProcessState', 'Wait', 'Continue']


class CancelledError(BaseException):
    """The process was cancelled."""


# region Commands
class Command(object):
    pass


class Cancel(Command):
    def __init__(self, msg=None):
        self.msg = msg


class Wait(Command):
    def __init__(self, continue_fn=None, msg=None):
        self.continue_fn = continue_fn
        self.msg = msg


class Stop(Command):
    def __init__(self, result):
        self.result = result


class Continue(Command):
    def __init__(self, continue_fn, *args, **kwargs):
        self.continue_fn = continue_fn
        self.args = args
        self.kwargs = kwargs


# endregion


class ProcessEvent(Enum):
    PLAY = 0
    PAUSE = 1
    WAIT = 2
    RESUME = 3
    FINISH = 4
    FAIL = 5
    CANCEL = 6


# region States

class ProcessState(Enum):
    """
    The possible states that a :class:`Process` can be in.
    """
    CREATED = 0
    RUNNING = 1
    WAITING = 2
    PAUSED = 3
    FINISHED = 4
    FAILED = 5
    CANCELLED = 6


class State(statemachine.State):
    INFO = 'INFO'
    # Can transition from any (non-terminal) state to these
    TRANSITIONS = {
        ProcessEvent.CANCEL: ProcessState.CANCELLED,
        ProcessEvent.FAIL: ProcessState.FAILED
    }

    @property
    def process(self):
        return self.state_machine

    def save_instance_state(self, out_state):
        pass

    def load_instance_state(self, process, saved_state):
        self.state_machine = process


class Created(State):
    LABEL = ProcessState.CREATED
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.CANCELLED,
               ProcessState.FAILED}
    TRANSITIONS = {
        ProcessEvent.PLAY: ProcessState.RUNNING,
    }

    def evt(self, event, *args, **kwargs):
        if event == ProcessEvent.PLAY:
            args = (self.process.run,)
        super(Created, self).evt(event, *args, **kwargs)


class Running(State):
    LABEL = ProcessState.RUNNING
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.WAITING,
               ProcessState.PAUSED,
               ProcessState.FINISHED,
               ProcessState.CANCELLED,
               ProcessState.FAILED}
    TRANSITIONS = {
        ProcessEvent.RESUME: ProcessState.RUNNING,
        ProcessEvent.WAIT: ProcessState.WAITING,
        ProcessEvent.PAUSE: ProcessState.PAUSED,
        ProcessEvent.FINISH: ProcessState.FINISHED,
        # TODO: Remove these once transition inheritance is supported
        ProcessEvent.CANCEL: ProcessState.CANCELLED,
        ProcessEvent.FAIL: ProcessState.FAILED
    }

    def __init__(self, process, run_fn, *args, **kwargs):
        super(Running, self).__init__(process)
        assert run_fn is not None
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs

    def _run(self):
        try:
            command = self.run_fn(*self.args, **self.kwargs)
        except BaseException as e:
            self.evt(ProcessEvent.FAIL, e)
        else:
            if not isinstance(command, Command):
                command = Stop(command)

            if isinstance(command, Cancel):
                self.evt(ProcessEvent.CANCEL, command.msg)
            if isinstance(command, Stop):
                self.evt(ProcessEvent.FINISH, command.result)
            elif isinstance(command, Wait):
                self.evt(ProcessEvent.WAIT, command.continue_fn, command.msg)
            elif isinstance(command, Continue):
                self.evt(ProcessEvent.RESUME, command.continue_fn, *command.args)


class Waiting(State):
    LABEL = ProcessState.WAITING
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.PAUSED,
               ProcessState.CANCELLED,
               ProcessState.FAILED}
    TRANSITIONS = {
        ProcessEvent.WAIT: ProcessState.WAITING,
        ProcessEvent.RESUME: ProcessState.RUNNING,
        ProcessEvent.PAUSE: ProcessState.PAUSED,
        ProcessEvent.FINISH: ProcessState.FINISHED
    }

    def __str__(self):
        state_info = super(Waiting, self).__str__()
        if self.msg is not None:
            state_info += " ({})".format(self.msg)
        return state_info

    def __init__(self, process, done_callback, msg=None, data=None):
        super(Waiting, self).__init__(process)
        self.done_callback = done_callback
        self.msg = msg
        self.data = data

    def evt(self, event, *args, **kwargs):
        if event == ProcessEvent.RESUME:
            args = (self.done_callback,)
        super(Waiting, self).evt(event, *args, **kwargs)


class Paused(State):
    LABEL = ProcessState.PAUSED
    ALLOWED = {ProcessState.WAITING,
               ProcessState.RUNNING,
               ProcessState.CANCELLED,
               ProcessState.FAILED}

    PAUSED_STATE = 'PAUSED_STATE'

    def __init__(self, process, paused_state):
        """
        :param process: The process this state belongs to
        :param paused_state: The state that was interrupted by pausing
        :type paused_state: :class:`State`
        """
        super(Paused, self).__init__(process)
        self.paused_state = paused_state

    def save_instance_state(self, out_state):
        super(Paused, self).save_instance_state(out_state)
        next_state_state = {}
        self.paused_state.save_instance_state(next_state_state)
        out_state[self.PAUSED_STATE] = next_state_state

    def load_instance_state(self, process, saved_state):
        super(Paused, self).load_instance_state(process, saved_state)
        self.paused_state = State.create_from(process, saved_state[self.PAUSED_STATE])

    def evt(self, event, *args, **kwargs):
        if event == ProcessEvent.PLAY:
            self.transition_to(self.paused_state)
        else:
            super(Paused, self).evt(event, *args, **kwargs)


class Failed(State):
    LABEL = ProcessState.FAILED

    def __init__(self, parent, exception):
        super(Failed, self).__init__(parent)
        self.exception = exception

    def __str__(self):
        exc = str(type(self.exception))
        try:
            exc += " - {}".format(self.exception.message)
        except AttributeError:
            pass
        return "{} ({})".format(self.LABEL, exc)


class Finished(State):
    LABEL = ProcessState.FINISHED

    RESULT = 'RESULT'

    def __init__(self, process, result):
        super(Finished, self).__init__(process)
        self.result = result

    def save_instance_state(self, out_state):
        super(Finished, self).save_instance_state(out_state)
        out_state[self.RESULT] = self.result

    def load_instance_state(self, process, saved_state):
        super(Finished, self).load_instance_state(process, saved_state)
        self.result = saved_state[self.RESULT]


class Cancelled(State):
    LABEL = ProcessState.CANCELLED
    MSG = 'MSG'

    def __init__(self, process, msg):
        super(Cancelled, self).__init__(process)
        self.msg = msg

    def save_instance_state(self, out_state):
        super(Cancelled, self).save_instance_state(out_state)
        out_state[self.MSG] = self.msg

    def load_instance_state(self, process, saved_state):
        super(Cancelled, self).load_instance_state(process, saved_state)
        self.msg = saved_state[self.MSG]


# endregion


class ProcessStateMachine(statemachine.StateMachine):
    """
    CREATED --- RUNNING --- STOPPED (o)
              /  v   ^     /
      PAUSED --- WAITING---


      * -- FAILED (o)
      * -- CANCELLED (o)

      * = any non terminal state
    """
    __metaclass__ = abc.ABCMeta

    STATES = (Created, Running, Waiting, Finished, Paused, Cancelled, Failed)

    def __str__(self):
        return "Process {}".format(self._state)

    def cancelled(self):
        return self.state == ProcessState.CANCELLED

    def cancelled_msg(self):
        if isinstance(self._state, Cancelled):
            return self._state.msg
        else:
            raise InvalidStateError("Has not been cancelled")

    def exception(self):
        if isinstance(self._state, Failed):
            return self._state.exception
        else:
            raise InvalidStateError("Has not failed")

    def done(self):
        """
        Return True if the call was successfully cancelled or finished running.
        :rtype: bool
        """
        return self.cancelled() or self.state == ProcessState.FINISHED

    @abc.abstractmethod
    def run(self):
        pass

    # region State entry/exit events

    def on_entered(self):
        state = self.state
        if state == ProcessState.CREATED:
            call_with_super_check(self.on_created)
        elif state == ProcessState.RUNNING:
            call_with_super_check(self.on_running)
        elif state == ProcessState.PAUSED:
            call_with_super_check(self.on_paused)
        elif state == ProcessState.WAITING:
            call_with_super_check(self.on_waiting, self._state.data)
        elif state == ProcessState.FINISHED:
            call_with_super_check(self.on_finished, self.result())
        elif state == ProcessState.FAILED:
            call_with_super_check(self.on_failed, self.exception())
        elif state == ProcessState.CANCELLED:
            call_with_super_check(self.on_cancelled, self._state.msg)

    def on_exiting(self):
        state = self.state
        if state == ProcessState.WAITING:
            call_with_super_check(self.on_exit_waiting)

    @super_check
    def on_created(self):
        pass

    @super_check
    def on_running(self):
        self._state._run()

    @super_check
    def on_waiting(self, data):
        pass

    @super_check
    def on_exit_waiting(self):
        pass

    @super_check
    def on_paused(self):
        pass

    @super_check
    def on_finished(self, result):
        pass

    @super_check
    def on_failed(self, exception):
        pass

    @super_check
    def on_cancelled(self, msg):
        pass

    # endregion

    def save_instance_state(self, out_state):
        self._state.save_instance_state(out_state)

    def load_instance_state(self, saved_state):
        pass

    def result(self):
        """
        Get the result from the process if it is finished.
        If the process was cancelled then a CancelledError will be raise.
        If the process has failed then the failing exception will be raised.
        If in any other state this will raise an InvalidStateError.
        :return: The result of the process
        """
        if isinstance(self._state, Finished):
            return self._state.result
        elif isinstance(self._state, Cancelled):
            raise CancelledError()
        elif isinstance(self._state, Failed):
            raise self._state.exception
        else:
            raise InvalidStateError

    # region commands
    def play(self):
        """
        Play the process if in the CREATED or PAUSED state
        """
        self.evt(ProcessEvent.PLAY)

    def pause(self):
        """ Pause the process """
        self.evt(ProcessEvent.PAUSE)

    def wait(self, done_callback=None, msg=None, data=None):
        """
        Wait for something
        :param done_callback: A function to call when done waiting
        :param msg: The waiting message
        :type msg: basestring
        :param data: Optional information about what to wait for
         """
        self.evt(ProcessEvent.WAIT, msg, data)

    def resume(self, value=None):
        """
        Start running the process again
        :param value: An optional value to pass to the callback function
        """
        self.evt(ProcessEvent.RESUME, value)

    def finish(self, result=None):
        """
        The process has finished
        :param result: An optional result from this process
        """
        self.evt(ProcessEvent.FINISH, result)

    def fail(self, exception):
        """
        Fail the process in response to an exception
        :param exception: The exception that caused the failure
        """
        self.evt(ProcessEvent.FAIL, exception)

    def cancel(self, msg=None):
        """
        Cancel the process
        :param msg: An optional cancellation message
        :type msg: basestring
        """
        self.evt(ProcessEvent.CANCEL, msg)


        # endregion
