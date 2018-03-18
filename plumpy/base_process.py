import abc
from builtins import str
import collections
from enum import Enum
import inspect
from future.utils import with_metaclass, raise_
import sys
import tornado.gen
import traceback
import yaml

from plumpy.futures import ensure_awaitable

try:
    import tblib

    _HAS_TBLIB = True
except ImportError:
    _HAS_TBLIB = False

from .base import state_machine
from .base.state_machine import InvalidStateError, event
from .base import super_check, call_with_super_check

from . import events
from . import futures
from . import persistence
from .persistence import auto_persist
from . import stack
from . import utils

__all__ = ['ProcessStateMachine', 'ProcessState', 'KilledError', 'UnsuccessfulResult',
           'Created', 'Running', 'Waiting', 'Finished', 'Excepted', 'Killed', 'InvalidStateError',
           # Commands
           'Kill', 'Stop', 'Wait', 'Continue']


class __NULL(object):
    def __eq__(self, other):
        return isinstance(other, self.__class__)


NULL = __NULL()


class KilledError(BaseException):
    """The process was killed."""


class UnsuccessfulResult(object):
    """The result of the process was unsuccessful"""

    def __init__(self, result=None):
        self.result = result


# region Commands


class Command(persistence.Savable):
    pass


@auto_persist('msg')
class Kill(Command):
    def __init__(self, msg=None):
        self.msg = msg


class Pause(Command):
    pass


@auto_persist('to_await', 'continue_fn', 'msg')
class Wait(Command):
    def __init__(self, awaitable, continue_fn, msg=None):
        self.to_await = futures.ensure_awaitable(awaitable)
        self.continue_fn = continue_fn
        self.msg = msg


@auto_persist('result')
class Stop(Command):
    def __init__(self, result, successful=True):
        self.result = result
        self.successful = successful


@auto_persist('args', 'kwargs')
class Continue(Command):
    CONTINUE_FN = 'continue_fn'

    def __init__(self, continue_fn, *args, **kwargs):
        self.continue_fn = continue_fn
        self.args = args
        self.kwargs = kwargs

    def save_instance_state(self, out_state):
        super(Continue, self).save_instance_state(out_state)
        # Either a free function of a method of process
        if inspect.ismethod(self.continue_fn):
            out_state[self.CONTINUE_FN] = self.continue_fn.__name__
        else:
            out_state[self.CONTINUE_FN] = utils.function_name(self.continue_fn)

    def load_instance_state(self, saved_state, load_context):
        super(Continue, self).load_instance_state(saved_state, load_context)
        # Either a free function or a method of process
        try:
            self.continue_fn = utils.load_function(saved_state[self.CONTINUE_FN])
        except ValueError:
            process = load_context.process
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
    EXCEPTED = 'excepted'
    KILLED = 'killed'


@auto_persist('in_state')
class State(state_machine.State, persistence.Savable):
    @property
    def process(self):
        """
        :return: The process
        :rtype: :class:`ProcessStateMachine`
        """
        return self.state_machine

    def load_instance_state(self, saved_state, load_context):
        super(State, self).load_instance_state(saved_state, load_context)
        self.state_machine = load_context.process

    def start(self):
        # Default response is to ignore start event
        return False

    def pause(self):
        # Cannot pause a terminal state
        return not self.is_terminal()

    def play(self):
        return True

    def kill(self, msg=None):
        self.transition_to(ProcessState.KILLED, msg)
        return True


@auto_persist('args', 'kwargs')
class Created(State):
    LABEL = ProcessState.CREATED
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.KILLED,
               ProcessState.EXCEPTED}

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

    def load_instance_state(self, saved_state, load_context):
        super(Created, self).load_instance_state(saved_state, load_context)
        self.run_fn = getattr(self.process, saved_state[self.RUN_FN])

    def start(self):
        # Make sure it's not paused
        self.process.play()
        # Now start
        self.transition_to(ProcessState.RUNNING, self.run_fn, *self.args, **self.kwargs)
        return True


class KillInterruption(Exception):
    pass


@auto_persist('args', 'kwargs')
class Running(State):
    LABEL = ProcessState.RUNNING
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.WAITING,
               ProcessState.FINISHED,
               ProcessState.KILLED,
               ProcessState.EXCEPTED}

    RUN_FN = 'run_fn'  # The key used to store the function to run
    COMMAND = 'command'  # The key used to store an upcoming command

    # Class level defaults
    _command = None
    _running = False
    _pausing = None
    _run_handle = None

    def __init__(self, process, run_fn, *args, **kwargs):
        super(Running, self).__init__(process)
        assert run_fn is not None
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs
        self._run_handle = None

    def enter(self):
        super(Running, self).enter()
        self._run_handle = self.process.call_soon(self._run)

    def exit(self):
        super(Running, self).exit()
        # Make sure the run callback doesn't get actioned if it wasn't already
        if self._run_handle is not None:
            self._run_handle.kill()
            self._run_handle = None

    def save_instance_state(self, out_state):
        super(Running, self).save_instance_state(out_state)
        out_state[self.RUN_FN] = self.run_fn.__name__
        if self._command is not None:
            out_state[self.COMMAND] = self._command.save()

    def load_instance_state(self, saved_state, load_context):
        super(Running, self).load_instance_state(saved_state, load_context)
        self.run_fn = getattr(self.process, saved_state[self.RUN_FN])
        if self.COMMAND in saved_state:
            self._command = persistence.Savable.load(saved_state[self.COMMAND], load_context)

        if self.in_state:
            self.play()

    def kill(self, message=None):
        if self._running:
            raise KillInterruption(message)
        else:
            return super(Running, self).kill(message)

    def play(self):
        if not self.in_state:
            raise RuntimeError("Cannot play when not in this state")
        if self._run_handle is not None:
            return False
        self._run_handle = self.process.call_soon(self._run)
        return True

    def pause(self):
        if self._running:
            if not self._pausing:
                self._pausing = futures.Future()
                self._pausing.add_done_callback(self._paused)
            return self._pausing
        else:
            return True

    def _run(self):
        self._run_handle = None
        with stack.in_stack(self.process):
            if self._command is not None:
                command = self._command
            else:
                try:
                    try:
                        self._running = True
                        result = self.run_fn(*self.args, **self.kwargs)
                    finally:
                        self._running = False
                except KillInterruption as e:
                    command = Kill(str(e))
                except BaseException:
                    self.transition_to(ProcessState.EXCEPTED, *sys.exc_info()[1:])
                    return
                else:
                    if not isinstance(result, Command):

                        if isinstance(result, UnsuccessfulResult):
                            result = Stop(result.result, False)
                        else:
                            result = Stop(result, True)

                    if self._pausing is not None:
                        self._command = result
                        self._pausing.set_result(True)
                        return
                    else:
                        command = result

            self._action_command(command)

    def _action_command(self, command):
        if isinstance(command, Kill):
            self.process.kill(command.msg)
        elif isinstance(command, Pause):
            self.pause()
        elif isinstance(command, Stop):
            self.transition_to(ProcessState.FINISHED, command.result, command.successful)
        elif isinstance(command, Wait):
            self.transition_to(ProcessState.WAITING, command.to_await, command.continue_fn, msg=command.msg)
        elif isinstance(command, Continue):
            self.transition_to(ProcessState.RUNNING, command.continue_fn, *command.args)
        else:
            raise ValueError("Unrecognised command")

    def _paused(self, future):
        assert future is self._pausing
        self._pausing = None


@auto_persist('msg', 'awaiting')
class Waiting(State):
    LABEL = ProcessState.WAITING
    ALLOWED = {ProcessState.RUNNING,
               ProcessState.WAITING,
               ProcessState.KILLED,
               ProcessState.EXCEPTED,
               ProcessState.FINISHED}

    DONE_CALLBACK = 'DONE_CALLBACK'

    def __str__(self):
        state_info = super(Waiting, self).__str__()
        if self.msg is not None:
            state_info += " ({})".format(self.msg)
        return state_info

    def __init__(self, process, awaitable, done_callback, msg=None):
        super(Waiting, self).__init__(process)
        if not futures.is_awaitable(awaitable):
            raise TypeError("Waiting state expects an awaitable")

        self.awaiting = awaitable
        self.done_callback = done_callback
        self.msg = msg
        self.process.loop().add_callback(self._await)

    def save_instance_state(self, out_state):
        super(Waiting, self).save_instance_state(out_state)
        if self.done_callback is not None:
            out_state[self.DONE_CALLBACK] = self.done_callback.__name__

    def load_instance_state(self, saved_state, load_context):
        # The 'instance' variable is used by the future it there is a
        # method that needs to be loaded
        load_context.copyextend(instance=load_context.process)
        super(Waiting, self).load_instance_state(saved_state, load_context)
        callback_name = saved_state.get(self.DONE_CALLBACK, None)
        if callback_name is not None:
            self.done_callback = getattr(self.process, callback_name)

        # Schedule the await callback
        self.process.loop().add_callback(self._await)

    @tornado.gen.coroutine
    def _await(self):
        try:
            result = yield self.awaiting
            self._resume(result)
        except Exception:
            exc_info = sys.exc_info()
            self.transition_to(ProcessState.EXCEPTED, exc_info[1], exc_info[2])

    def _resume(self, result):
        if self._callback_accepts_result():
            self.transition_to(ProcessState.RUNNING, self.done_callback, result)
        else:
            self.transition_to(ProcessState.RUNNING, self.done_callback)

    def _callback_accepts_result(self):
        args, varargs, keywords, defaults = inspect.getargspec(self.done_callback)
        if len(args) == 2:
            return True
        elif keywords and len(args) + len(keywords) >= 2:
            return True
        else:
            return False


class Excepted(State):
    LABEL = ProcessState.EXCEPTED

    EXC_VALUE = 'ex_value'
    TRACEBACK = 'traceback'

    def __init__(self, process, exception, trace_back=None):
        """
        :param process: The associated process
        :param exception: The exception instance
        :param trace_back: An optional exception traceback
        """
        super(Excepted, self).__init__(process)
        self.exception = exception
        self.traceback = trace_back

    def __str__(self):
        return "{} ({})".format(
            super(Excepted, self).__str__(),
            traceback.format_exception_only(type(self.exception), self.exception)[0]
        )

    def save_instance_state(self, out_state):
        super(Excepted, self).save_instance_state(out_state)
        out_state[self.EXC_VALUE] = yaml.dump(self.exception)
        if self.traceback is not None:
            out_state[self.TRACEBACK] = "".join(traceback.format_tb(self.traceback))

    def load_instance_state(self, saved_state, load_context):
        super(Excepted, self).load_instance_state(saved_state, load_context)
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


@auto_persist('result', 'successful')
class Finished(State):
    LABEL = ProcessState.FINISHED

    def __init__(self, process, result, successful):
        super(Finished, self).__init__(process)
        self.result = result
        self.successful = successful


@auto_persist('msg')
class Killed(State):
    LABEL = ProcessState.KILLED

    def __init__(self, process, msg):
        """
        :param process: The associated process
        :param msg: Optional kill message
        :type msg: str
        """
        super(Killed, self).__init__(process)
        self.msg = msg


# endregion


class ProcessStateMachineMeta(abc.ABCMeta, state_machine.StateMachineMeta):
    pass

# Make ProcessStateMachineMeta instances (classes) YAML - able
yaml.representer.Representer.add_representer(
    ProcessStateMachineMeta,
    yaml.representer.Representer.represent_name
)

@persistence.auto_persist('_paused_flag')
class ProcessStateMachine(with_metaclass(ProcessStateMachineMeta,
                                         state_machine.StateMachine,
                                         persistence.Savable)):
    """
                  ___
                 |   v
    CREATED --- RUNNING --- FINISHED (o)
                 |   ^     /
                 v   |    /
                 WAITING--
                 |   ^
                  ----


      * -- EXCEPTED (o)
      * -- KILLED (o)

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
            ProcessState.EXCEPTED: Excepted,
            ProcessState.KILLED: Killed
        }

    def __init__(self, loop=None):
        super(ProcessStateMachine, self).__init__()
        self._loop = loop if loop is not None else events.get_event_loop()

        self._paused_flag = False
        self._pausing = None  # If pausing, this will be a future

    @property
    def paused(self):
        return self._paused

    @property
    def _paused(self):
        return self._paused_flag

    @_paused.setter
    def _paused(self, paused):
        if self._paused == paused:
            return

        # We are changing the paused state
        self._paused_flag = paused
        if self._paused_flag:
            call_with_super_check(self.on_pause)
        else:
            call_with_super_check(self.on_play)

    # region loop methods

    def loop(self):
        return self._loop

    def call_soon(self, callback, *args, **kwargs):
        """
        Schedule a callback to what is considered an internal process function
        (this needn't be a method).  If it raises an exception it will cause
        the process to fail.
        """
        handle = events.Handle(self, callback, args, kwargs)
        self._loop.add_callback(handle._run)
        return handle

    def call_soon_external(self, callback, *args, **kwargs):
        """
        Schedule a callback to an external method.  If there is an
        exception in the callback it will not cause the process to fail.
        """
        self._loop.add_callback(callback, *args, **kwargs)

    def callback_excepted(self, callback, exception, trace):
        if self.state != ProcessState.EXCEPTED:
            self.fail(exception, trace)

    # endregion

    def create_initial_state(self):
        return self.get_state_class(ProcessState.CREATED)(self, self.run)

    def killed(self):
        return self.state == ProcessState.KILLED

    def killed_msg(self):
        if isinstance(self._state, Killed):
            return self._state.msg
        else:
            raise InvalidStateError("Has not been killed")

    def exception(self):
        if isinstance(self._state, Excepted):
            return self._state.exception
        else:
            return None

    def done(self):
        """
        Return True if the call was successfully killed or finished running.
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
            call_with_super_check(self.on_wait, state)
        elif state_label == ProcessState.FINISHED:
            call_with_super_check(self.on_finish, state.result, state.successful)
        elif state_label == ProcessState.KILLED:
            call_with_super_check(self.on_kill, state.msg)
        elif state_label == ProcessState.EXCEPTED:
            call_with_super_check(self.on_except, state.get_exc_info())

    def on_exiting(self):
        super(ProcessStateMachine, self).on_exiting()

        state = self.state
        if state == ProcessState.WAITING:
            call_with_super_check(self.on_exit_waiting)
        elif state == ProcessState.RUNNING:
            call_with_super_check(self.on_exit_running)

    def transition_excepted(self, initial_state, final_state, exception, trace):
        # If we are creating, then reraise instead of failing.
        if final_state == ProcessState.CREATED:
            raise_(type(exception), exception, trace)
        else:
            self.transition_to(ProcessState.EXCEPTED, exception, trace)

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
    def on_wait(self, state):
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
    def on_finish(self, result, successful):
        pass

    @super_check
    def on_except(self, exc_info):
        pass

    @super_check
    def on_kill(self, sg):
        pass

    # endregion

    def save_instance_state(self, out_state):
        super(ProcessStateMachine, self).save_instance_state(out_state)
        out_state['_state'] = self._state.save()

    def load_instance_state(self, saved_state, load_context):
        super(ProcessStateMachine, self).load_instance_state(saved_state, load_context)

        if 'loop' in load_context:
            self._loop = load_context.loop
        else:
            self._loop = events.get_event_loop()

        self._pausing = None  # If pausing, this will be a future
        self._state = self.create_state(saved_state['_state'])

    def result(self):
        """
        Get the result from the process if it is finished.
        If the process was killed then a KilledError will be raise.
        If the process has excepted then the failing exception will be raised.
        If in any other state this will raise an InvalidStateError.
        :return: The result of the process
        """
        if isinstance(self._state, Finished):
            return self._state.result
        elif isinstance(self._state, Killed):
            raise KilledError()
        elif isinstance(self._state, Excepted):
            raise self._state.exception
        else:
            raise InvalidStateError

    def successful(self):
        """
        Returns whether the result of the process is considered successful
        Will raise if the process is not in the FINISHED state
        """
        try:
            return self._state.successful
        except AttributeError:
            raise InvalidStateError('process is not in the finished state')

    # region commands
    @event(to_states=(Running, Waiting, Excepted))
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

    @event(to_states=Excepted)
    def fail(self, exception, trace_back=None):
        """
        Fail the process in response to an exception
        :param exception: The exception that caused the failure
        :param trace_back: Optional exception traceback
        """
        self.transition_to(ProcessState.EXCEPTED, exception, trace_back)

    @event(to_states=(Killed, Excepted))
    def kill(self, msg=None):
        """
        Kill the process
        :param msg: An optional kill message
        :type msg: str
        """
        return self._state.kill(msg)

        # endregion

    def create_state(self, saved_state):
        """
        Create a state object from a saved state

        :param saved_state: The saved state
        :type saved_state: :class:`Bundle`
        :return: An instance of the object with its state loaded from the save state.
        """
        load_context = persistence.LoadContext(process=self)
        return persistence.Savable.load(saved_state, load_context)
