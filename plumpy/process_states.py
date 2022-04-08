# -*- coding: utf-8 -*-
import asyncio
from enum import Enum
import sys
import traceback
from types import TracebackType
from typing import TYPE_CHECKING, Any, Callable, Optional, Tuple, Type, Union, cast

import yaml
from yaml.loader import Loader

try:
    import tblib

    _HAS_TBLIB: bool = True
except ImportError:
    _HAS_TBLIB = False

from . import exceptions, futures, persistence, utils
from .base import state_machine
from .lang import NULL
from .persistence import auto_persist
from .utils import SAVED_STATE_TYPE

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

if TYPE_CHECKING:
    from .processes import Process  # pylint: disable=cyclic-import


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

    def __init__(self, msg: Optional[Any] = None):
        super().__init__()
        self.msg = msg


class Pause(Command):
    pass


@auto_persist('msg', 'data')
class Wait(Command):

    def __init__(
        self, continue_fn: Optional[Callable[..., Any]] = None, msg: Optional[Any] = None, data: Optional[Any] = None
    ):
        super().__init__()
        self.continue_fn = continue_fn
        self.msg = msg
        self.data = data


@auto_persist('result')
class Stop(Command):

    def __init__(self, result: Any, successful: bool) -> None:
        super().__init__()
        self.result = result
        self.successful = successful


@auto_persist('args', 'kwargs')
class Continue(Command):
    CONTINUE_FN = 'continue_fn'

    def __init__(self, continue_fn: Callable[..., Any], *args: Any, **kwargs: Any):
        super().__init__()
        self.continue_fn = continue_fn
        self.args = args
        self.kwargs = kwargs

    def save_instance_state(self, out_state: SAVED_STATE_TYPE, save_context: persistence.LoadSaveContext) -> None:
        super().save_instance_state(out_state, save_context)
        out_state[self.CONTINUE_FN] = self.continue_fn.__name__

    def load_instance_state(self, saved_state: SAVED_STATE_TYPE, load_context: persistence.LoadSaveContext) -> None:
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
    The possible states that a :class:`~plumpy.processes.Process` can be in.
    """
    CREATED: str = 'created'
    RUNNING: str = 'running'
    WAITING: str = 'waiting'
    FINISHED: str = 'finished'
    EXCEPTED: str = 'excepted'
    KILLED: str = 'killed'


@auto_persist('in_state')
class State(state_machine.State, persistence.Savable):

    @property
    def process(self) -> state_machine.StateMachine:
        """
        :return: The process
        """
        return self.state_machine

    def load_instance_state(self, saved_state: SAVED_STATE_TYPE, load_context: persistence.LoadSaveContext) -> None:
        super().load_instance_state(saved_state, load_context)
        self.state_machine = load_context.process

    def interrupt(self, reason: Any) -> None:  # pylint: disable=unused-argument
        pass


@auto_persist('args', 'kwargs')
class Created(State):
    LABEL = ProcessState.CREATED
    ALLOWED = {ProcessState.RUNNING, ProcessState.KILLED, ProcessState.EXCEPTED}

    RUN_FN = 'run_fn'

    def __init__(self, process: 'Process', run_fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        super().__init__(process)
        assert run_fn is not None
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs

    def save_instance_state(self, out_state: SAVED_STATE_TYPE, save_context: persistence.LoadSaveContext) -> None:
        super().save_instance_state(out_state, save_context)
        out_state[self.RUN_FN] = self.run_fn.__name__

    def load_instance_state(self, saved_state: SAVED_STATE_TYPE, load_context: persistence.LoadSaveContext) -> None:
        super().load_instance_state(saved_state, load_context)
        self.run_fn = getattr(self.process, saved_state[self.RUN_FN])

    def execute(self) -> state_machine.State:
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
    _command: Union[None, Kill, Stop, Wait, Continue] = None
    _running: bool = False
    _run_handle = None

    def __init__(self, process: 'Process', run_fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        super().__init__(process)
        assert run_fn is not None
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs
        self._run_handle = None

    def save_instance_state(self, out_state: SAVED_STATE_TYPE, save_context: persistence.LoadSaveContext) -> None:
        super().save_instance_state(out_state, save_context)
        out_state[self.RUN_FN] = self.run_fn.__name__
        if self._command is not None:
            out_state[self.COMMAND] = self._command.save()

    def load_instance_state(self, saved_state: SAVED_STATE_TYPE, load_context: persistence.LoadSaveContext) -> None:
        super().load_instance_state(saved_state, load_context)
        self.run_fn = getattr(self.process, saved_state[self.RUN_FN])
        if self.COMMAND in saved_state:
            self._command = persistence.Savable.load(saved_state[self.COMMAND], load_context)  # type: ignore

    def interrupt(self, reason: Any) -> None:
        pass

    async def execute(self) -> State:  # type: ignore # pylint: disable=invalid-overridden-method
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
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                # note this re-raise is only required in python<=3.7,
                # for python>=3.8 asyncio.CancelledError does not inherit from Exception,
                # so will not be caught below
                raise
            except Exception:  # pylint: disable=broad-except
                excepted = self.create_state(ProcessState.EXCEPTED, *sys.exc_info()[1:])
                return cast(State, excepted)
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

    def _action_command(self, command: Union[Kill, Stop, Wait, Continue]) -> State:
        if isinstance(command, Kill):
            state = self.create_state(ProcessState.KILLED, command.msg)
        # elif isinstance(command, Pause):
        #     self.pause()
        elif isinstance(command, Stop):
            state = self.create_state(ProcessState.FINISHED, command.result, command.successful)
        elif isinstance(command, Wait):
            state = self.create_state(ProcessState.WAITING, command.continue_fn, command.msg, command.data)
        elif isinstance(command, Continue):
            state = self.create_state(ProcessState.RUNNING, command.continue_fn, *command.args)
        else:
            raise ValueError('Unrecognised command')

        return cast(State, state)  # casting from base.State to process.State


@auto_persist('msg', 'data')
class Waiting(State):
    LABEL = ProcessState.WAITING
    ALLOWED = {
        ProcessState.RUNNING, ProcessState.WAITING, ProcessState.KILLED, ProcessState.EXCEPTED, ProcessState.FINISHED
    }

    DONE_CALLBACK = 'DONE_CALLBACK'

    _interruption = None

    def __str__(self) -> str:
        state_info = super().__str__()
        if self.msg is not None:
            state_info += f' ({self.msg})'
        return state_info

    def __init__(
        self,
        process: 'Process',
        done_callback: Optional[Callable[..., Any]],
        msg: Optional[str] = None,
        data: Optional[Any] = None
    ) -> None:
        super().__init__(process)
        self.done_callback = done_callback
        self.msg = msg
        self.data = data
        self._waiting_future: futures.Future = futures.Future()

    def save_instance_state(self, out_state: SAVED_STATE_TYPE, save_context: persistence.LoadSaveContext) -> None:
        super().save_instance_state(out_state, save_context)
        if self.done_callback is not None:
            out_state[self.DONE_CALLBACK] = self.done_callback.__name__

    def load_instance_state(self, saved_state: SAVED_STATE_TYPE, load_context: persistence.LoadSaveContext) -> None:
        super().load_instance_state(saved_state, load_context)
        callback_name = saved_state.get(self.DONE_CALLBACK, None)
        if callback_name is not None:
            self.done_callback = getattr(self.process, callback_name)
        else:
            self.done_callback = None
        self._waiting_future = futures.Future()

    def interrupt(self, reason: Any) -> None:
        # This will cause the future in execute() to raise the exception
        self._waiting_future.set_exception(reason)

    async def execute(self) -> State:  # type: ignore # pylint: disable=invalid-overridden-method
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

        return cast(State, next_state)  # casting from base.State to process.State

    def resume(self, value: Any = NULL) -> None:
        assert self._waiting_future is not None, 'Not yet waiting'
        self._waiting_future.set_result(value)


class Excepted(State):
    LABEL = ProcessState.EXCEPTED

    EXC_VALUE = 'ex_value'
    TRACEBACK = 'traceback'

    def __init__(
        self, process: 'Process', exception: Optional[BaseException], trace_back: Optional[TracebackType] = None
    ):
        """
        :param process: The associated process
        :param exception: The exception instance
        :param trace_back: An optional exception traceback
        """
        super().__init__(process)
        self.exception = exception
        self.traceback = trace_back

    def __str__(self) -> str:
        exception = traceback.format_exception_only(type(self.exception) if self.exception else None, self.exception)[0]
        return super().__str__() + f'({exception})'

    def save_instance_state(self, out_state: SAVED_STATE_TYPE, save_context: persistence.LoadSaveContext) -> None:
        super().save_instance_state(out_state, save_context)
        out_state[self.EXC_VALUE] = yaml.dump(self.exception)
        if self.traceback is not None:
            out_state[self.TRACEBACK] = ''.join(traceback.format_tb(self.traceback))

    def load_instance_state(self, saved_state: SAVED_STATE_TYPE, load_context: persistence.LoadSaveContext) -> None:
        super().load_instance_state(saved_state, load_context)
        self.exception = yaml.load(saved_state[self.EXC_VALUE], Loader=Loader)
        if _HAS_TBLIB:
            try:
                self.traceback = \
                    tblib.Traceback.from_string(saved_state[self.TRACEBACK],
                                                strict=False)
            except KeyError:
                self.traceback = None
        else:
            self.traceback = None

    def get_exc_info(self) -> Tuple[Optional[Type[BaseException]], Optional[BaseException], Optional[TracebackType]]:
        """
        Recreate the exc_info tuple and return it
        """
        return type(self.exception) if self.exception else None, self.exception, self.traceback


@auto_persist('result', 'successful')
class Finished(State):
    LABEL = ProcessState.FINISHED

    def __init__(self, process: 'Process', result: Any, successful: bool) -> None:
        super().__init__(process)
        self.result = result
        self.successful = successful


@auto_persist('msg')
class Killed(State):
    LABEL = ProcessState.KILLED

    def __init__(self, process: 'Process', msg: Optional[str]):
        """
        :param process: The associated process
        :param msg: Optional kill message

        """
        super().__init__(process)
        self.msg = msg


# endregion
