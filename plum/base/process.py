import abc
from enum import Enum
import six
import sys
import traceback
import yaml

try:
    import tblib

    _HAS_TBLIB = True
except ImportError:
    _HAS_TBLIB = False

from . import state_machine
from .state_machine import InvalidStateError, event
from .utils import super_check, call_with_super_check, flag

__all__ = ['ProcessStateMachine', 'ProcessState',
           'Created', 'Running', 'Waiting', 'Paused', 'Finished', 'Failed',
           'Cancelled',
           # Commands
           'Cancel', 'Stop', 'Wait', 'Continue']


class __NULL(object):
    def __eq__(self, other):
        return isinstance(other, self.__class__)


NULL = __NULL()


class CancelledError(BaseException):
    """The process was cancelled."""


# region Commands
class Command(object):
    pass


class Cancel(Command):
    def __init__(self, msg=None):
        self.msg = msg


class Pause(Command):
    pass


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


# region States

class ProcessState(Enum):
    """
    The possible states that a :class:`Process` can be in.
    """
    CREATED = 'created'
    RUNNING = 'running'
    WAITING = 'waiting'
    PAUSED = 'paused'
    FINISHED = 'finished'
    FAILED = 'failed'
    CANCELLED = 'cancelled'


KEY_STATE = 'state'
KEY_IN_STATE = 'in_state'


class State(state_machine.State):
    @property
    def process(self):
        return self.state_machine

    def save_instance_state(self, out_state):
        out_state[KEY_STATE] = self.LABEL
        out_state[KEY_IN_STATE] = self.in_state

    @super_check
    def load_instance_state(self, process, saved_state):
        self.state_machine = process
        self.in_state = saved_state[KEY_IN_STATE]

    def cancel(self, msg=None):
        self.transition_to(ProcessState.CANCELLED, msg)


class Created(State):
    LABEL = ProcessState.CREATED
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.CANCELLED,
               ProcessState.FAILED}

    def __init__(self, process, run_fn, *args, **kwargs):
        super(Created, self).__init__(process)
        assert run_fn is not None
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs

    def play(self):
        self.transition_to(ProcessState.RUNNING, self.run_fn, *self.args, **self.kwargs)


class CancelInterruption(BaseException):
    pass


class Running(State):
    LABEL = ProcessState.RUNNING
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.WAITING,
               ProcessState.PAUSED,
               ProcessState.FINISHED,
               ProcessState.CANCELLED,
               ProcessState.FAILED}

    RUN_FN = 'run_fn'
    ARGS = 'args'
    KWARGS = 'kwargs'
    _command = None
    _running = False

    def __init__(self, process, run_fn, *args, **kwargs):
        super(Running, self).__init__(process)
        assert run_fn is not None
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs

    def save_instance_state(self, out_state):
        super(Running, self).save_instance_state(out_state)
        out_state[self.RUN_FN] = self.run_fn.__name__
        out_state[self.ARGS] = self.args
        out_state[self.KWARGS] = self.kwargs

    def load_instance_state(self, process, saved_state):
        super(Running, self).load_instance_state(process, saved_state)
        self.run_fn = getattr(self.process, saved_state[self.RUN_FN])
        self.args = saved_state[self.ARGS]
        self.kwargs = saved_state[self.KWARGS]

    def cancel(self, message=None):
        if self._running:
            raise CancelInterruption(message)
        else:
            super(Running, self).cancel(message)

    def pause(self):
        if self._running:
            if self._command is None:
                self._command = Pause()
            return state_machine.EventResponse.DELAYED
        else:
            self.transition_to(ProcessState.PAUSED, self)

    def resume(self, run_fn, value=NULL):
        if value == NULL:
            self.transition_to(ProcessState.RUNNING, run_fn)
        else:
            self.transition_to(ProcessState.RUNNING, run_fn, value)

    def _run(self):
        if self._command is not None:
            command = self._command
        else:
            try:
                try:
                    self._running = True
                    result = self.run_fn(*self.args, **self.kwargs)
                finally:
                    self._running = False
            except CancelInterruption as e:
                command = Cancel(e.message)
            except BaseException:
                self.process.fail(*sys.exc_info()[1:])
                return
            else:
                if not isinstance(result, Command):
                    result = Stop(result)

                if self._command is not None:
                    # Overwrite with the command we got while running
                    command = self._command
                    self._command = result
                else:
                    command = result

        self._action_command(command)

    def _action_command(self, command):
        if isinstance(command, Cancel):
            self.process.cancel(command.msg)
        elif isinstance(command, Pause):
            self.pause()
        elif isinstance(command, Stop):
            self.process.finish(command.result)
        elif isinstance(command, Wait):
            self.process.wait(command.continue_fn, command.msg)
        elif isinstance(command, Continue):
            self.process.resume(command.continue_fn, *command.args)
        else:
            raise ValueError("Unrecognised command")


class Waiting(State):
    LABEL = ProcessState.WAITING
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.PAUSED,
               ProcessState.CANCELLED,
               ProcessState.FAILED}

    DONE_CALLBACK = 'DONE_CALLBACK'
    MSG = 'MSG'
    DATA = 'DATA'

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

    def save_instance_state(self, out_state):
        super(Waiting, self).save_instance_state(out_state)
        out_state[self.DONE_CALLBACK] = self.done_callback.__name__
        out_state[self.MSG] = self.msg
        out_state[self.DATA] = self.data

    def load_instance_state(self, process, saved_state):
        super(Waiting, self).load_instance_state(process, saved_state)
        self.done_callback = getattr(self.process, saved_state[self.DONE_CALLBACK])
        self.msg = saved_state[self.MSG]
        self.data = saved_state[self.DATA]

    def pause(self):
        self.transition_to(ProcessState.PAUSED, self)

    def resume(self, value=NULL):
        if value == NULL:
            self.transition_to(ProcessState.RUNNING, self.done_callback)
        else:
            self.transition_to(ProcessState.RUNNING, self.done_callback, value)


class Paused(State):
    LABEL = ProcessState.PAUSED
    ALLOWED = {ProcessState.WAITING,
               ProcessState.RUNNING,
               ProcessState.CANCELLED,
               ProcessState.FAILED}

    PAUSED_STATE = 'PAUSED_STATE'

    def __init__(self, process, paused_state):
        """
        :param process: The associated process
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
        self.paused_state = process.create_state(saved_state[self.PAUSED_STATE])

    def play(self):
        self.transition_to(self.paused_state)


class Failed(State):
    LABEL = ProcessState.FAILED

    EXC_VALUE = 'ex_value'
    TRACEBACK = 'traceback'

    def __init__(self, process, exception, trace_back=None):
        """
        :param process: The associated process
        :param exception: The exception instance
        :param trace_back: An optional exception traceback
        """
        super(Failed, self).__init__(process)
        self.exception = exception
        self.traceback = trace_back

    def __str__(self):
        return "{} ({})".format(
            super(Failed, self).__str__(),
            traceback.format_exception_only(type(self.exception), self.exception)[0]
        )

    def save_instance_state(self, out_state):
        super(Failed, self).save_instance_state(out_state)
        out_state[self.EXC_VALUE] = yaml.dump(self.exception)
        if self.traceback is not None:
            out_state[self.TRACEBACK] = "".join(traceback.format_tb(self.traceback))

    def load_instance_state(self, process, saved_state):
        super(Failed, self).load_instance_state(process, saved_state)
        self.exception = yaml.load(saved_state[self.EXC_VALUE])
        if _HAS_TBLIB:
            try:
                self.traceback = \
                    tblib.Traceback.from_string(saved_state[self.TRACEBACK],
                                                strict=False)
            except KeyError:
                self.traceback = None
        else:
            self.traceback = None

    def get_exc_info(self):
        """
        Recreate the exc_info tuple and return it
        """
        return (type(self.exception), self.exception, self.traceback)


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
        """
        :param process: The associated process
        :param msg: Optional cancellation message
        :type msg: basestring
        """
        super(Cancelled, self).__init__(process)
        self.msg = msg

    def save_instance_state(self, out_state):
        super(Cancelled, self).save_instance_state(out_state)
        out_state[self.MSG] = self.msg

    def load_instance_state(self, process, saved_state):
        super(Cancelled, self).load_instance_state(process, saved_state)
        self.msg = saved_state[self.MSG]


# endregion


class ProcessStateMachineMeta(abc.ABCMeta, state_machine.StateMachineMeta):
    pass


@six.add_metaclass(ProcessStateMachineMeta)
class ProcessStateMachine(state_machine.StateMachine):
    """
    CREATED --- RUNNING --- FINISHED (o)
              /  |   ^      /
             /   v   |     /
      PAUSED --- WAITING---


      * -- FAILED (o)
      * -- CANCELLED (o)

      * = any non terminal state
    """

    @classmethod
    def get_states(cls):
        state_classes = cls.get_state_classes()
        return (state_classes[ProcessState.CREATED],) + \
               tuple(state
                     for state in state_classes.values()
                     if state.LABEL != ProcessState.CREATED)

    @classmethod
    def get_state_classes(cls):
        # A mapping of the State constants to the corresponding state class
        return {
            ProcessState.CREATED: Created,
            ProcessState.RUNNING: Running,
            ProcessState.WAITING: Waiting,
            ProcessState.PAUSED: Paused,
            ProcessState.FINISHED: Finished,
            ProcessState.FAILED: Failed,
            ProcessState.CANCELLED: Cancelled
        }

    def create_initial_state(self):
        return self.get_state_class(ProcessState.CREATED)(self, self.run)

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
            call_with_super_check(self.on_failed, self._state.get_exc_info())
        elif state == ProcessState.CANCELLED:
            call_with_super_check(self.on_cancelled, self._state.msg)

    def on_exiting(self):
        state = self.state
        if state == ProcessState.WAITING:
            call_with_super_check(self.on_exit_waiting)
        elif state == ProcessState.RUNNING:
            call_with_super_check(self.on_exit_running)

    @super_check
    def on_created(self):
        pass

    @super_check
    def on_running(self):
        pass

    @super_check
    def on_exit_running(self):
        pass

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
    def on_failed(self, exc_info):
        pass

    @super_check
    def on_cancelled(self, msg):
        pass

    # endregion

    def save_state(self, out_state):
        return call_with_super_check(self.save_instance_state, out_state)

    def load_state(self, saved_state, *args, **kwargs):
        return call_with_super_check(
            self.load_instance_state, saved_state, *args, **kwargs)

    @super_check
    def save_instance_state(self, out_state):
        self._state.save_instance_state(out_state)

    @super_check
    def load_instance_state(self, saved_state):
        self._state = self.create_state(saved_state)

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
    @event(from_states=(Created, Paused), to_states=(Running, Waiting))
    def play(self):
        """
        Play the process if in the CREATED or PAUSED state
        """
        return self._state.play()

    @event(from_states=(Running, Waiting), to_states=Paused)
    def pause(self):
        """ Pause the process """
        return self._state.pause()

    @event(from_states=Running, to_states=Waiting)
    def wait(self, done_callback=None, msg=None, data=None):
        """
        Wait for something
        :param done_callback: A function to call when done waiting
        :param msg: The waiting message
        :type msg: basestring
        :param data: Optional information about what to wait for
         """
        self.transition_to(ProcessState.WAITING, done_callback, msg, data)

    @event(from_states=(Running, Waiting), to_states=Running)
    def resume(self, *args):
        """
        Start running the process again
        """
        return self._state.resume(*args)

    @event(from_states=(Running, Waiting), to_states=Finished)
    def finish(self, result=None):
        """
        The process has finished
        :param result: An optional result from this process
        """
        self.transition_to(ProcessState.FINISHED, result)

    @event(to_states=Failed)
    def fail(self, exception, trace_back=None):
        """
        Fail the process in response to an exception
        :param exception: The exception that caused the failure
        :param trace_back: Optional exception traceback
        """
        self.transition_to(ProcessState.FAILED, exception, trace_back)

    @event(to_states=Cancelled)
    def cancel(self, msg=None):
        """
        Cancel the process
        :param msg: An optional cancellation message
        :type msg: basestring
        """
        return self._state.cancel(msg)

        # endregion

    def create_state(self, saved_state):
        """
        Create a state object from a saved state

        :param saved_state: The saved state
        :type saved_state: :class:`Bundle`
        :return: An instance of the object with its state loaded from the save state.
        """
        # Get the class using the class loader and instantiate it
        state_label = ProcessState(saved_state[KEY_STATE])
        state_class = self._ensure_state_class(state_label)
        state = state_class.__new__(state_class)
        call_with_super_check(state.load_instance_state, self, saved_state)
        return state
