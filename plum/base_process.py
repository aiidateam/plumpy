import abc
from builtins import str
from enum import Enum
from future.utils import with_metaclass, raise_
import sys
import traceback
import yaml

try:
    import tblib

    _HAS_TBLIB = True
except ImportError:
    _HAS_TBLIB = False

from plum.base import state_machine
from plum.base.state_machine import InvalidStateError, event
from plum.base import super_check, call_with_super_check

from . import futures
from . import persisters
from plum.persisters import auto_persist
from . import utils

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


class Command(persisters.Savable):
    pass


@auto_persist('msg')
class Cancel(Command):
    def __init__(self, msg=None):
        self.msg = msg


class Pause(Command):
    pass


@auto_persist('msg', 'data')
class Wait(Command):
    def __init__(self, continue_fn=None, msg=None, data=None):
        self.continue_fn = continue_fn
        self.msg = msg
        self.data = data


@auto_persist('result')
class Stop(Command):
    def __init__(self, result):
        self.result = result


@auto_persist('args', 'kwargs')
class Continue(Command):
    CONTINUE_FN = 'continue_fn'

    def __init__(self, continue_fn, *args, **kwargs):
        self.continue_fn = continue_fn
        self.args = args
        self.kwargs = kwargs

    def save_instance_state(self, out_state):
        super(Continue, self).save_instance_state(out_state)
        out_state[self.CONTINUE_FN] = self.continue_fn.__name__

    def load_instance_state(self, saved_state, process):
        super(Continue, self).load_instance_state(saved_state)
        try:
            self.continue_fn = utils.load_function(saved_state[self.CONTINUE_FN])
        except ValueError:
            self.continue_fn = getattr(process, saved_state[self.CONTINUE_FN])


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


@auto_persist('in_state')
class State(state_machine.State, persisters.Savable):
    @property
    def process(self):
        return self.state_machine

    def load_instance_state(self, saved_state, process):
        super(State, self).load_instance_state(saved_state)
        self.state_machine = process

    def play(self):
        return state_machine.EventResponse.IGNORED

    def cancel(self, msg=None):
        self.transition_to(ProcessState.CANCELLED, msg)
        return True


@auto_persist('args', 'kwargs')
class Created(State):
    LABEL = ProcessState.CREATED
    ALLOWED = {ProcessState.PAUSED,
               ProcessState.RUNNING,
               ProcessState.CANCELLED,
               ProcessState.FAILED}

    RUN_FN = 'run_fn'

    def __init__(self, process, run_fn, *args, **kwargs):
        super(Created, self).__init__(process)
        assert run_fn is not None
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs

    def save_instance_state(self, out_state):
        super(Created, self).save_instance_state(out_state)
        out_state[self.RUN_FN] = self.run_fn.__name__

    def load_instance_state(self, saved_state, process):
        super(Created, self).load_instance_state(saved_state, process)
        self.run_fn = getattr(self.process, saved_state[self.RUN_FN])

    def pause(self):
        self.transition_to(ProcessState.PAUSED, self)
        return True

    def play(self):
        self.transition_to(ProcessState.RUNNING, self.run_fn, *self.args, **self.kwargs)
        return True


class CancelInterruption(Exception):
    pass


@auto_persist('args', 'kwargs')
class Running(State):
    LABEL = ProcessState.RUNNING
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.WAITING,
               ProcessState.PAUSED,
               ProcessState.FINISHED,
               ProcessState.CANCELLED,
               ProcessState.FAILED}

    RUN_FN = 'run_fn'
    COMMAND = 'command'

    _command = None
    _running = False
    _interruption = None

    def __init__(self, process, run_fn, *args, **kwargs):
        super(Running, self).__init__(process)
        assert run_fn is not None
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs

    def save_instance_state(self, out_state):
        super(Running, self).save_instance_state(out_state)
        out_state[self.RUN_FN] = self.run_fn.__name__
        if self._command is not None:
            out_state[self.COMMAND] = self._command.save()

    def load_instance_state(self, saved_state, process):
        super(Running, self).load_instance_state(saved_state, process)
        self.run_fn = getattr(self.process, saved_state[self.RUN_FN])
        if self.COMMAND in saved_state:
            self._command = persisters.Savable.load(saved_state[self.COMMAND], self.process)

    def cancel(self, message=None):
        if self._running:
            raise CancelInterruption(message)
        else:
            return super(Running, self).cancel(message)

    def pause(self):
        if self._running:
            if self._interruption is not None:
                self._interruption.cancel()
            self._interruption = futures.Future()
            self._command = Pause()
            return self._interruption
        else:
            self.transition_to(ProcessState.PAUSED, self)
            return True

    def resume(self, run_fn, value=NULL):
        if value == NULL:
            self.transition_to(ProcessState.RUNNING, run_fn)
        else:
            self.transition_to(ProcessState.RUNNING, run_fn, value)
        return True

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
                command = Cancel(str(e))
            except BaseException:
                self.process.fail(*sys.exc_info()[1:])
                return
            else:
                if not isinstance(result, Command):
                    result = Stop(result)

                if self._interruption is not None:
                    # Use the command from the interruption and save the
                    # new one for later
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
            self.process.wait(command.continue_fn, command.msg, command.data)
        elif isinstance(command, Continue):
            self.process.resume(command.continue_fn, *command.args)
        else:
            raise ValueError("Unrecognised command")


@auto_persist('msg', 'data')
class Waiting(State):
    LABEL = ProcessState.WAITING
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.PAUSED,
               ProcessState.WAITING,
               ProcessState.CANCELLED,
               ProcessState.FAILED}

    DONE_CALLBACK = 'DONE_CALLBACK'

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
        if self.done_callback is not None:
            out_state[self.DONE_CALLBACK] = self.done_callback.__name__

    def load_instance_state(self, saved_state, process):
        super(Waiting, self).load_instance_state(saved_state, process)
        callback_name = saved_state.get(self.DONE_CALLBACK, None)
        if callback_name is not None:
            self.done_callback = getattr(self.process, callback_name)

    def pause(self):
        self.transition_to(ProcessState.PAUSED, self)
        return True

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
        out_state[self.PAUSED_STATE] = self.paused_state.save()

    def load_instance_state(self, saved_state, process):
        super(Paused, self).load_instance_state(saved_state, process)
        self.paused_state = process.create_state(saved_state[self.PAUSED_STATE])

    def play(self):
        if self.paused_state.LABEL == ProcessState.CREATED:
            # Go direct to the next state
            self.paused_state.play()
        else:
            self.transition_to(self.paused_state)
        return True


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

    def load_instance_state(self, saved_state, process):
        super(Failed, self).load_instance_state(saved_state, process)
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
        return type(self.exception), self.exception, self.traceback


@auto_persist('result')
class Finished(State):
    LABEL = ProcessState.FINISHED

    def __init__(self, process, result):
        super(Finished, self).__init__(process)
        self.result = result


@auto_persist('msg')
class Cancelled(State):
    LABEL = ProcessState.CANCELLED

    def __init__(self, process, msg):
        """
        :param process: The associated process
        :param msg: Optional cancellation message
        :type msg: str
        """
        super(Cancelled, self).__init__(process)
        self.msg = msg


# endregion


class ProcessStateMachineMeta(abc.ABCMeta, state_machine.StateMachineMeta):
    pass


class ProcessStateMachine(with_metaclass(ProcessStateMachineMeta,
                                         state_machine.StateMachine,
                                         persisters.Savable)):
    """
                  ___
                 |   v
    CREATED --- RUNNING --- FINISHED (o)
        |     /  |   ^      /
        v    /   v   |     /
      PAUSED --- WAITING---
                 |   ^
                  ----


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
        return self._state.is_terminal()

    @abc.abstractmethod
    def run(self):
        pass

    # region State entry/exit events

    def on_entering(self, state):
        super(ProcessStateMachine, self).on_entering(state)

        state_label = state.LABEL
        if state_label == ProcessState.CREATED:
            call_with_super_check(self.on_create)
        elif state_label == ProcessState.RUNNING:
            call_with_super_check(self.on_run)
        elif state_label == ProcessState.PAUSED:
            call_with_super_check(self.on_pause)
        elif state_label == ProcessState.WAITING:
            call_with_super_check(self.on_wait, state.data)
        elif state_label == ProcessState.FINISHED:
            call_with_super_check(self.on_finish, state.result)
        elif state_label == ProcessState.CANCELLED:
            call_with_super_check(self.on_cancel, state.msg)
        elif state_label == ProcessState.FAILED:
            call_with_super_check(self.on_fail, state.get_exc_info())

    def on_exiting(self):
        super(ProcessStateMachine, self).on_exiting()

        state = self.state
        if state == ProcessState.PAUSED:
            call_with_super_check(self.on_exit_paused)
        if state == ProcessState.WAITING:
            call_with_super_check(self.on_exit_waiting)
        elif state == ProcessState.RUNNING:
            call_with_super_check(self.on_exit_running)

    def transition_failed(self, initial_state, final_state, exception, trace):
        # If we are creating, then reraise instead of failing.
        if final_state == ProcessState.CREATED:
            raise_(type(exception), exception, trace)
        else:
            self.fail(exception, trace)

    @super_check
    def on_create(self):
        pass

    @super_check
    def on_run(self):
        pass

    @super_check
    def on_exit_running(self):
        pass

    @super_check
    def on_wait(self, data):
        pass

    @super_check
    def on_exit_waiting(self):
        pass

    @super_check
    def on_pause(self):
        pass

    @super_check
    def on_paused(self):
        pass

    @super_check
    def on_exit_paused(self):
        pass

    @super_check
    def on_finish(self, result):
        pass

    @super_check
    def on_fail(self, exc_info):
        pass

    @super_check
    def on_cancel(self, sg):
        pass

    # endregion

    def save_state(self, out_state):
        return call_with_super_check(self.save_instance_state, out_state)

    def save_instance_state(self, out_state):
        super(ProcessStateMachine, self).save_instance_state(out_state)
        out_state['_state'] = self._state.save()

    def load_instance_state(self, saved_state):
        super(ProcessStateMachine, self).load_instance_state(saved_state)
        self._state = self.create_state(saved_state['_state'])

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
    @event(to_states=(Running, Waiting, Failed))
    def play(self):
        """
        Play the process if in the CREATED or PAUSED state
        """
        return self._state.play()

    @event(from_states=(Created, Running, Waiting), to_states=(Paused, Failed))
    def pause(self):
        """ Pause the process """
        return self._state.pause()

    @event(from_states=(Running, Waiting), to_states=(Waiting, Failed))
    def wait(self, done_callback=None, msg=None, data=None):
        """
        Wait for something
        :param done_callback: A function to call when done waiting
        :param msg: The waiting message
        :type msg: str
        :param data: Optional information about what to wait for
         """
        self.transition_to(ProcessState.WAITING, done_callback, msg, data)

    @event(from_states=(Running, Waiting), to_states=(Running, Failed))
    def resume(self, *args):
        """
        Start running the process again
        """
        return self._state.resume(*args)

    @event(from_states=(Running, Waiting), to_states=(Finished, Failed))
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

    @event(to_states=(Cancelled, Failed))
    def cancel(self, msg=None):
        """
        Cancel the process
        :param msg: An optional cancellation message
        :type msg: str
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
        return persisters.Savable.load(saved_state, self)
