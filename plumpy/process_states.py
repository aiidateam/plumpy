from builtins import str
from enum import Enum
import sys
import tornado.concurrent
from tornado.gen import coroutine, Return
import traceback
import yaml

try:
    import tblib

    _HAS_TBLIB = True
except ImportError:
    _HAS_TBLIB = False

from . import futures
from .base import state_machine
from . import persistence
from .persistence import auto_persist
from . import utils
from . import exceptions

__all__ = [
    'ProcessState',
    'Created',
    'Running',
    'Waiting',
    'Finished',
    'Excepted',
    'Killed',
    # Commands
    'Kill',
    'Stop',
    'Wait',
    'Continue'
]


class __NULL(object):
    def __eq__(self, other):
        return isinstance(other, self.__class__)


NULL = __NULL()


class Interrupt(Enum):
    """ Interrupt a state with a reason code """
    PAUSE = 0
    KILL = 9


class KillInterruption(Exception):
    pass


# region Commands


class Command(persistence.Savable):
    pass


@auto_persist('msg')
class Kill(Command):
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
    def __init__(self, result, successful):
        self.result = result
        self.successful = successful


@auto_persist('args', 'kwargs')
class Continue(Command):
    CONTINUE_FN = 'continue_fn'

    def __init__(self, continue_fn, *args, **kwargs):
        self.continue_fn = continue_fn
        self.args = args
        self.kwargs = kwargs

    def save_instance_state(self, out_state, save_context):
        super(Continue, self).save_instance_state(out_state, save_context)
        out_state[self.CONTINUE_FN] = self.continue_fn.__name__

    def load_instance_state(self, saved_state, load_context):
        super(Continue, self).load_instance_state(saved_state, load_context)
        try:
            self.continue_fn = utils.load_function(
                saved_state[self.CONTINUE_FN])
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

    def interrupt(self, reason):
        return False


@auto_persist('args', 'kwargs')
class Created(State):
    LABEL = ProcessState.CREATED
    ALLOWED = {
        ProcessState.RUNNING, ProcessState.KILLED, ProcessState.EXCEPTED
    }

    RUN_FN = 'run_fn'

    def __init__(self, process, run_fn, *args, **kwargs):
        super(Created, self).__init__(process)
        assert run_fn is not None
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs

    def save_instance_state(self, out_state, save_context):
        super(Created, self).save_instance_state(out_state, save_context)
        out_state[self.RUN_FN] = self.run_fn.__name__

    def load_instance_state(self, saved_state, load_context):
        super(Created, self).load_instance_state(saved_state, load_context)
        self.run_fn = getattr(self.process, saved_state[self.RUN_FN])

    def execute(self):
        return self.create_state(ProcessState.RUNNING, self.run_fn, *self.args,
                                 **self.kwargs)


@auto_persist('args', 'kwargs')
class Running(State):
    LABEL = ProcessState.RUNNING
    ALLOWED = {
        ProcessState.RUNNING, ProcessState.WAITING, ProcessState.FINISHED,
        ProcessState.KILLED, ProcessState.EXCEPTED
    }

    RUN_FN = 'run_fn'  # The key used to store the function to run
    COMMAND = 'command'  # The key used to store an upcoming command

    # Class level defaults
    _command = None
    _running = False
    _run_handle = None

    def __init__(self, process, run_fn, *args, **kwargs):
        super(Running, self).__init__(process)
        assert run_fn is not None
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs
        self._run_handle = None

    def save_instance_state(self, out_state, save_context):
        super(Running, self).save_instance_state(out_state, save_context)
        out_state[self.RUN_FN] = self.run_fn.__name__
        if self._command is not None:
            out_state[self.COMMAND] = self._command.save()

    def load_instance_state(self, saved_state, load_context):
        super(Running, self).load_instance_state(saved_state, load_context)
        self.run_fn = getattr(self.process, saved_state[self.RUN_FN])
        if self.COMMAND in saved_state:
            self._command = persistence.Savable.load(saved_state[self.COMMAND],
                                                     load_context)

    def interrupt(self, reason):
        if self._running and reason is Interrupt.KILL:
            raise KillInterruption()
        else:
            return False

    @coroutine
    def execute(self):
        if self._command is not None:
            command = self._command
        else:
            try:
                try:
                    self._running = True
                    result = self.run_fn(*self.args, **self.kwargs)
                finally:
                    self._running = False
            except KillInterruption:
                # Let this bubble up to the caller
                raise
            except Exception:
                excepted = self.create_state(ProcessState.EXCEPTED,
                                             *sys.exc_info()[1:])
                raise Return(excepted)
            else:
                if not isinstance(result, Command):
                    if isinstance(result, exceptions.UnsuccessfulResult):
                        result = Stop(result.result, False)
                    else:
                        # Got passed a basic return type
                        result = Stop(result, True)

                command = result

        next_state = self._action_command(command)
        raise Return(next_state)

    def _action_command(self, command):
        if isinstance(command, Kill):
            return self.create_state(ProcessState.FINISHED, command.result,
                                     command.successful)
        # elif isinstance(command, Pause):
        #     self.pause()
        elif isinstance(command, Stop):
            return self.create_state(ProcessState.FINISHED, command.result,
                                     command.successful)
        elif isinstance(command, Wait):
            return self.create_state(ProcessState.WAITING, command.continue_fn,
                                     command.msg, command.data)
        elif isinstance(command, Continue):
            return self.create_state(ProcessState.RUNNING, command.continue_fn,
                                     *command.args)
        else:
            raise ValueError("Unrecognised command")


@auto_persist('msg', 'data')
class Waiting(State):
    LABEL = ProcessState.WAITING
    ALLOWED = {
        ProcessState.RUNNING, ProcessState.WAITING, ProcessState.KILLED,
        ProcessState.EXCEPTED, ProcessState.FINISHED
    }

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
        self._waiting_future = futures.Future()

    def save_instance_state(self, out_state, save_context):
        super(Waiting, self).save_instance_state(out_state, save_context)
        if self.done_callback is not None:
            out_state[self.DONE_CALLBACK] = self.done_callback.__name__

    def load_instance_state(self, saved_state, load_context):
        super(Waiting, self).load_instance_state(saved_state, load_context)
        callback_name = saved_state.get(self.DONE_CALLBACK, None)
        if callback_name is not None:
            self.done_callback = getattr(self.process, callback_name)
        self._waiting_future = futures.Future()

    def interrupt(self, reason):
        # This will cause the future in execute() to raise the exception
        self._waiting_future.set_exception(KillInterruption())
        # Reset the future for the next time
        self._waiting_future = futures.Future()

    @coroutine
    def execute(self):
        result = yield self._waiting_future
        if result == NULL:
            next_state = self.create_state(ProcessState.RUNNING,
                                           self.done_callback)
        else:
            next_state = self.create_state(ProcessState.RUNNING,
                                           self.done_callback, result)

        raise Return(next_state)

    def resume(self, value=NULL):
        self._waiting_future.set_result(value)


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
            traceback.format_exception_only(
                type(self.exception), self.exception)[0])

    def save_instance_state(self, out_state, save_context):
        super(Excepted, self).save_instance_state(out_state, save_context)
        out_state[self.EXC_VALUE] = yaml.dump(self.exception)
        if self.traceback is not None:
            out_state[self.TRACEBACK] = "".join(
                traceback.format_tb(self.traceback))

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
