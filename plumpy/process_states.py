# -*- coding: utf-8 -*-
from enum import Enum
import sys
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
from .lang import NULL
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
    'Continue',
    'Interruption',
    'KillInterruption',
    'PauseInterruption',
]


class Interruption(Exception):
    pass


class KillInterruption(Interruption):
    pass


class PauseInterruption(Interruption):
    pass


# region Commands


class Command(persistence.Savable):
    pass


@auto_persist('msg')
class Kill(Command):

    def __init__(self, msg=None):
        super().__init__()
        self.msg = msg


class Pause(Command):
    pass


@auto_persist('msg', 'data')
class Wait(Command):

    def __init__(self, continue_fn=None, msg=None, data=None):
        super().__init__()
        self.continue_fn = continue_fn
        self.msg = msg
        self.data = data


@auto_persist('result')
class Stop(Command):

    def __init__(self, result, successful):
        super().__init__()
        self.result = result
        self.successful = successful


@auto_persist('args', 'kwargs')
class Continue(Command):
    CONTINUE_FN = 'continue_fn'

    def __init__(self, continue_fn, *args, **kwargs):
        super().__init__()
        self.continue_fn = continue_fn
        self.args = args
        self.kwargs = kwargs

    def save_instance_state(self, out_state, save_context):
        super().save_instance_state(out_state, save_context)
        out_state[self.CONTINUE_FN] = self.continue_fn.__name__

    def load_instance_state(self, saved_state, load_context):
        super().load_instance_state(saved_state, load_context)
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
        super().load_instance_state(saved_state, load_context)
        self.state_machine = load_context.process

    @staticmethod
    def interrupt(reason):  # pylint: disable=unused-argument
        return False


@auto_persist('args', 'kwargs')
class Created(State):
    LABEL = ProcessState.CREATED
    ALLOWED = {ProcessState.RUNNING, ProcessState.KILLED, ProcessState.EXCEPTED}

    RUN_FN = 'run_fn'

    def __init__(self, process, run_fn, *args, **kwargs):
        super().__init__(process)
        assert run_fn is not None
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs

    def save_instance_state(self, out_state, save_context):
        super().save_instance_state(out_state, save_context)
        out_state[self.RUN_FN] = self.run_fn.__name__

    def load_instance_state(self, saved_state, load_context):
        super().load_instance_state(saved_state, load_context)
        self.run_fn = getattr(self.process, saved_state[self.RUN_FN])

    def execute(self):
        return self.create_state(ProcessState.RUNNING, self.run_fn, *self.args, **self.kwargs)


@auto_persist('args', 'kwargs')
class Running(State):
    LABEL = ProcessState.RUNNING
    ALLOWED = {
        ProcessState.RUNNING, ProcessState.WAITING, ProcessState.FINISHED, ProcessState.KILLED, ProcessState.EXCEPTED
    }

    RUN_FN = 'run_fn'  # The key used to store the function to run
    COMMAND = 'command'  # The key used to store an upcoming command

    # Class level defaults
    _command = None
    _running = False
    _run_handle = None

    def __init__(self, process, run_fn, *args, **kwargs):
        super().__init__(process)
        assert run_fn is not None
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs
        self._run_handle = None
        # import pdb; pdb.set_trace()

    def save_instance_state(self, out_state, save_context):
        super().save_instance_state(out_state, save_context)
        out_state[self.RUN_FN] = self.run_fn.__name__
        if self._command is not None:
            out_state[self.COMMAND] = self._command.save()

    def load_instance_state(self, saved_state, load_context):
        super().load_instance_state(saved_state, load_context)
        self.run_fn = getattr(self.process, saved_state[self.RUN_FN])
        if self.COMMAND in saved_state:
            self._command = persistence.Savable.load(saved_state[self.COMMAND], load_context)

    def interrupt(self, reason):
        return False

    async def execute(self):
        if self._command is not None:
            command = self._command
        else:
            try:
                try:
                    self._running = True
                    result = self.run_fn(*self.args, **self.kwargs)
                finally:
                    self._running = False
            except Interruption:
                # Let this bubble up to the caller
                raise
            except Exception:  # pylint: disable=broad-except
                excepted = self.create_state(ProcessState.EXCEPTED, *sys.exc_info()[1:])
                return excepted
            else:
                if not isinstance(result, Command):
                    if isinstance(result, exceptions.UnsuccessfulResult):
                        result = Stop(result.result, False)
                    else:
                        # Got passed a basic return type
                        result = Stop(result, True)

                command = result

        next_state = self._action_command(command)
        return next_state

    def _action_command(self, command):
        if isinstance(command, Kill):
            return self.create_state(ProcessState.FINISHED, command.result, command.successful)
        # elif isinstance(command, Pause):
        #     self.pause()
        if isinstance(command, Stop):
            return self.create_state(ProcessState.FINISHED, command.result, command.successful)

        if isinstance(command, Wait):
            return self.create_state(ProcessState.WAITING, command.continue_fn, command.msg, command.data)

        if isinstance(command, Continue):
            return self.create_state(ProcessState.RUNNING, command.continue_fn, *command.args)

        raise ValueError('Unrecognised command')


@auto_persist('msg', 'data')
class Waiting(State):
    LABEL = ProcessState.WAITING
    ALLOWED = {
        ProcessState.RUNNING, ProcessState.WAITING, ProcessState.KILLED, ProcessState.EXCEPTED, ProcessState.FINISHED
    }

    DONE_CALLBACK = 'DONE_CALLBACK'

    _interruption = None

    def __str__(self):
        state_info = super().__str__()
        if self.msg is not None:
            state_info += ' ({})'.format(self.msg)
        return state_info

    def __init__(self, process, done_callback, msg=None, data=None):
        super().__init__(process)
        self.done_callback = done_callback
        self.msg = msg
        self.data = data
        self._waiting_future = futures.Future()

    def save_instance_state(self, out_state, save_context):
        super().save_instance_state(out_state, save_context)
        if self.done_callback is not None:
            out_state[self.DONE_CALLBACK] = self.done_callback.__name__

    def load_instance_state(self, saved_state, load_context):
        super().load_instance_state(saved_state, load_context)
        callback_name = saved_state.get(self.DONE_CALLBACK, None)
        if callback_name is not None:
            self.done_callback = getattr(self.process, callback_name)
        else:
            self.done_callback = None
        self._waiting_future = futures.Future()

    def interrupt(self, reason):
        # This will cause the future in execute() to raise the exception
        self._waiting_future.set_exception(reason)

    async def execute(self):
        try:
            result = await self._waiting_future
        except Interruption:
            # Deal with the interruption (by raising) but make sure our internal
            # state is back to how it was before the interruption so that we can be
            # re-executed
            self._waiting_future = futures.Future()
            raise

        if result == NULL:
            next_state = self.create_state(ProcessState.RUNNING, self.done_callback)
        else:
            next_state = self.create_state(ProcessState.RUNNING, self.done_callback, result)

        return next_state

    def resume(self, value=NULL):
        assert self._waiting_future is not None, 'Not yet waiting'
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
        super().__init__(process)
        self.exception = exception
        self.traceback = trace_back

    def __str__(self):
        return '{} ({})'.format(
            super().__str__(),
            traceback.format_exception_only(type(self.exception), self.exception)[0]
        )

    def save_instance_state(self, out_state, save_context):
        super().save_instance_state(out_state, save_context)
        out_state[self.EXC_VALUE] = yaml.dump(self.exception)
        if self.traceback is not None:
            out_state[self.TRACEBACK] = ''.join(traceback.format_tb(self.traceback))

    def load_instance_state(self, saved_state, load_context):
        super().load_instance_state(saved_state, load_context)
        self.exception = yaml.load(saved_state[self.EXC_VALUE], Loader=yaml.FullLoader)
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
        super().__init__(process)
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
        super().__init__(process)
        self.msg = msg


# endregion
