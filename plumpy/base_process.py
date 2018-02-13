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

from .base import state_machine
from .base.state_machine import InvalidStateError, event
from .base import super_check, call_with_super_check

from . import futures
from . import persistence
from .persistence import auto_persist
from . import utils

__all__ = ['ProcessStateMachine', 'ProcessState',
           'Created', 'Running', 'Waiting', 'Finished', 'Failed',
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


class Command(persistence.Savable):
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
        super(Continue, self).load_instance_state(saved_state, process)
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
    FINISHED = 'finished'
    FAILED = 'failed'
    CANCELLED = 'cancelled'


@auto_persist('in_state')
class State(state_machine.State, persistence.Savable):
    @property
    def process(self):
        return self.state_machine

    def load_instance_state(self, saved_state, process):
        super(State, self).load_instance_state(saved_state, process)
        self.state_machine = process

    def start(self):
        # Default response is to ignore start event
        return False

    def pause(self):
        # Cannot pause a terminal state
        return not self.is_terminal()

    def play(self):
        return True

    def cancel(self, msg=None):
        self.transition_to(ProcessState.CANCELLED, msg)
        return True


@auto_persist('args', 'kwargs')
class Created(State):
    LABEL = ProcessState.CREATED
    ALLOWED = {ProcessState.RUNNING,
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

    def start(self):
        self.transition_to(ProcessState.RUNNING, self.run_fn, *self.args, **self.kwargs)
        return True


class CancelInterruption(Exception):
    pass


@auto_persist('args', 'kwargs')
class Running(State):
    LABEL = ProcessState.RUNNING
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.WAITING,
               ProcessState.FINISHED,
               ProcessState.CANCELLED,
               ProcessState.FAILED}

    RUN_FN = 'run_fn'
    COMMAND = 'command'

    _command = None
    _running = False
    _pausing = None

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
            self._command = persistence.Savable.load(saved_state[self.COMMAND], self.process)

    def cancel(self, message=None):
        if self._running:
            raise CancelInterruption(message)
        else:
            return super(Running, self).cancel(message)

    def pause(self):
        if self._running:
            if not self._pausing:
                self._pausing = futures.Future()
            return self._pausing
        else:
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
                self.transition_to(ProcessState.FAILED, *sys.exc_info()[1:])
                return
            else:
                if not isinstance(result, Command):
                    result = Stop(result)

                if self._pausing is not None:
                    self._command = result
                    self._pausing.set_result(True)
                    return
                else:
                    command = result

        self._action_command(command)

    def _action_command(self, command):
        if isinstance(command, Cancel):
            self.process.cancel(command.msg)
        elif isinstance(command, Pause):
            self.pause()
        elif isinstance(command, Stop):
            self.transition_to(ProcessState.FINISHED, command.result)
        elif isinstance(command, Wait):
            self.transition_to(ProcessState.WAITING, command.continue_fn, command.msg, command.data)
        elif isinstance(command, Continue):
            self.transition_to(ProcessState.RUNNING, command.continue_fn, *command.args)
        else:
            raise ValueError("Unrecognised command")


@auto_persist('msg', 'data')
class Waiting(State):
    LABEL = ProcessState.WAITING
    ALLOWED = {ProcessState.RUNNING,
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

    def resume(self, value=NULL):
        if value == NULL:
            self.transition_to(ProcessState.RUNNING, self.done_callback)
        else:
            self.transition_to(ProcessState.RUNNING, self.done_callback, value)
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
                                         persistence.Savable)):
    """
                  ___
                 |   v
    CREATED --- RUNNING --- FINISHED (o)
                 |   ^      /
                 v   |     /
                 WAITING---
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
            ProcessState.FINISHED: Finished,
            ProcessState.FAILED: Failed,
            ProcessState.CANCELLED: Cancelled
        }

    def __init__(self):
        super(ProcessStateMachine, self).__init__()
        self.__paused = False
        self._pausing = None  # If pausing, this will be a future

    @property
    def paused(self):
        return self._paused

    @property
    def _paused(self):
        return self.__paused

    @_paused.setter
    def _paused(self, paused):
        if self._paused == paused:
            return

        # We are changing the paused state
        self.__paused = paused
        if self.__paused:
            call_with_super_check(self.on_pause)
        else:
            call_with_super_check(self.on_play)

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
            return None

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
        if state == ProcessState.WAITING:
            call_with_super_check(self.on_exit_waiting)
        elif state == ProcessState.RUNNING:
            call_with_super_check(self.on_exit_running)

    def transition_failed(self, initial_state, final_state, exception, trace):
        # If we are creating, then reraise instead of failing.
        if final_state == ProcessState.CREATED:
            raise_(type(exception), exception, trace)
        else:
            self.transition_to(ProcessState.FAILED, exception, trace)

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
    def on_play(self):
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

    def save_instance_state(self, out_state):
        super(ProcessStateMachine, self).save_instance_state(out_state)
        out_state['_state'] = self._state.save()

    def load_instance_state(self, saved_state, load_context):
        super(ProcessStateMachine, self).load_instance_state(saved_state, load_context)
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
    def start(self):
        """
        Start the process if in the CREATED state
        """
        return self._state.start()

    def pause(self):
        """
        Pause the process.  Returns True if after this call the process is paused, False otherwise

        :return: True paused, False otherwise
        """
        if self._paused:
            # Already paused
            return True
        if self._pausing:
            return self._pausing

        state_paused = self._state.pause()
        if isinstance(state_paused, futures.Future):
            # The state is pausing itself
            self._pausing = state_paused

            def paused(future):
                # Finished pausing the state, check what the outcome was
                self._pausing = None
                if not (future.cancelled() or future.exception()):
                    self._paused = future.result()

            state_paused.add_done_callback(paused)
            return state_paused
        else:
            self._paused = state_paused
            return self._paused

    def play(self):
        """
        Play a process. Returns True if after this call the process is playing, False otherwise

        :return: True if playing, False otherwise
        """
        if not self._paused:
            if self._pausing:
                self._pausing.cancel()
            return True

        if self._state.play():
            self._paused = False

        return not self._paused

    @event(from_states=(Running, Waiting), to_states=(Running, Failed))
    def resume(self, *args):
        """
        Start running the process again
        """
        return self._state.resume(*args)

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
        return persistence.Savable.load(saved_state, self)
