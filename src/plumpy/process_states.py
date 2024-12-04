# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import traceback
from enum import Enum
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Optional,
    Protocol,
    Tuple,
    Type,
    Union,
    cast,
    final,
    runtime_checkable,
)

import yaml
from yaml.loader import Loader

from plumpy.message import MessageBuilder, MessageType

try:
    import tblib

    _HAS_TBLIB: bool = True
except ImportError:
    _HAS_TBLIB = False

from . import exceptions, futures, persistence, utils
from .base import state_machine as st
from .lang import NULL
from .persistence import LoadSaveContext, auto_persist
from .utils import SAVED_STATE_TYPE, ensure_coroutine

if TYPE_CHECKING:
    from .processes import Process


class Interruption(Exception):  # noqa: N818
    pass


class KillInterruption(Interruption):
    def __init__(self, msg_text: str | None):
        super().__init__()
        msg = MessageBuilder.kill(text=msg_text)

        self.msg: MessageType = msg


class PauseInterruption(Interruption):
    def __init__(self, msg_text: str | None):
        super().__init__()
        msg = MessageBuilder.pause(text=msg_text)

        self.msg: MessageType = msg


# region Commands


class Command(persistence.Savable):
    pass


@auto_persist('msg')
class Kill(Command):
    def __init__(self, msg: Optional[MessageType] = None):
        super().__init__()
        self.msg = msg


class Pause(Command):
    pass


@auto_persist('msg', 'data')
class Wait(Command):
    def __init__(
        self,
        continue_fn: Optional[Callable[..., Any]] = None,
        msg: Optional[Any] = None,
        data: Optional[Any] = None,
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
        self.state_machine = load_context.process
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

    CREATED = 'created'
    RUNNING = 'running'
    WAITING = 'waiting'
    FINISHED = 'finished'
    EXCEPTED = 'excepted'
    KILLED = 'killed'


@runtime_checkable
class Savable(Protocol):
    def save(self, save_context: LoadSaveContext | None = None) -> SAVED_STATE_TYPE: ...


@final
@auto_persist('args', 'kwargs')
class Created(persistence.Savable):
    LABEL: ClassVar = ProcessState.CREATED
    ALLOWED: ClassVar = {ProcessState.RUNNING, ProcessState.KILLED, ProcessState.EXCEPTED}

    RUN_FN = 'run_fn'
    is_terminal: ClassVar[bool] = False

    def __init__(self, process: 'Process', run_fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        assert run_fn is not None
        self.process = process
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs

    def save_instance_state(self, out_state: SAVED_STATE_TYPE, save_context: persistence.LoadSaveContext) -> None:
        super().save_instance_state(out_state, save_context)
        out_state[self.RUN_FN] = self.run_fn.__name__

    def load_instance_state(self, saved_state: SAVED_STATE_TYPE, load_context: persistence.LoadSaveContext) -> None:
        super().load_instance_state(saved_state, load_context)
        self.process = load_context.process

        self.run_fn = getattr(self.process, saved_state[self.RUN_FN])

    def execute(self) -> st.State:
        return st.create_state(
            self.process, ProcessState.RUNNING, process=self.process, run_fn=self.run_fn, *self.args, **self.kwargs
        )

    def enter(self) -> None: ...

    def exit(self) -> None: ...


@final
@auto_persist('args', 'kwargs')
class Running(persistence.Savable):
    LABEL: ClassVar = ProcessState.RUNNING
    ALLOWED: ClassVar = {
        ProcessState.RUNNING,
        ProcessState.WAITING,
        ProcessState.FINISHED,
        ProcessState.KILLED,
        ProcessState.EXCEPTED,
    }

    RUN_FN = 'run_fn'  # The key used to store the function to run
    COMMAND = 'command'  # The key used to store an upcoming command

    # Class level defaults
    _command: Union[None, Kill, Stop, Wait, Continue] = None
    _running: bool = False
    _run_handle = None

    is_terminal: ClassVar[bool] = False

    def __init__(
        self, process: 'Process', run_fn: Callable[..., Union[Awaitable[Any], Any]], *args: Any, **kwargs: Any
    ) -> None:
        assert run_fn is not None
        self.process = process
        self.run_fn = ensure_coroutine(run_fn)
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
        self.process = load_context.process

        self.run_fn = ensure_coroutine(getattr(self.process, saved_state[self.RUN_FN]))
        if self.COMMAND in saved_state:
            self._command = persistence.Savable.load(saved_state[self.COMMAND], load_context)  # type: ignore

    def interrupt(self, reason: Any) -> None:
        pass

    async def execute(self) -> st.State:
        if self._command is not None:
            command = self._command
        else:
            try:
                try:
                    self._running = True
                    result = await self.run_fn(*self.args, **self.kwargs)
                finally:
                    self._running = False
            except Interruption:
                # Let this bubble up to the caller
                raise
            except Exception:
                _, exception, traceback = sys.exc_info()
                # excepted = state_cls(exception=exception, traceback=traceback)
                excepted = Excepted(exception=exception, traceback=traceback)
                return excepted
            else:
                if not isinstance(result, Command):
                    if isinstance(result, exceptions.UnsuccessfulResult):
                        result = Stop(result.result, False)
                    else:
                        # Got passed a basic return type
                        result = Stop(result, True)

                command = cast(Stop, result)

        next_state = self._action_command(command)
        return next_state

    def _action_command(self, command: Union[Kill, Stop, Wait, Continue]) -> st.State:
        if isinstance(command, Kill):
            state = st.create_state(self.process, ProcessState.KILLED, msg=command.msg)
        # elif isinstance(command, Pause):
        #     self.pause()
        elif isinstance(command, Stop):
            state = st.create_state(
                self.process, ProcessState.FINISHED, result=command.result, successful=command.successful
            )
        elif isinstance(command, Wait):
            state = st.create_state(
                self.process,
                ProcessState.WAITING,
                process=self.process,
                done_callback=command.continue_fn,
                msg=command.msg,
                data=command.data,
            )
        elif isinstance(command, Continue):
            state = st.create_state(
                self.process,
                ProcessState.RUNNING,
                process=self.process,
                run_fn=command.continue_fn,
                *command.args,
                **command.kwargs,
            )
        else:
            raise ValueError('Unrecognised command')

        return state

    def enter(self) -> None: ...

    def exit(self) -> None: ...


@auto_persist('msg', 'data')
class Waiting(persistence.Savable):
    LABEL: ClassVar = ProcessState.WAITING
    ALLOWED: ClassVar = {
        ProcessState.RUNNING,
        ProcessState.WAITING,
        ProcessState.KILLED,
        ProcessState.EXCEPTED,
        ProcessState.FINISHED,
    }

    DONE_CALLBACK = 'DONE_CALLBACK'

    _interruption = None

    is_terminal: ClassVar[bool] = False

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
        data: Optional[Any] = None,
    ) -> None:
        self.process = process
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
        self.process = load_context.process

        callback_name = saved_state.get(self.DONE_CALLBACK, None)
        if callback_name is not None:
            self.done_callback = getattr(self.process, callback_name)
        else:
            self.done_callback = None
        self._waiting_future = futures.Future()

    def interrupt(self, reason: Exception) -> None:
        # This will cause the future in execute() to raise the exception
        self._waiting_future.set_exception(reason)

    async def execute(self) -> st.State:
        try:
            result = await self._waiting_future
        except Interruption:
            # Deal with the interruption (by raising) but make sure our internal
            # state is back to how it was before the interruption so that we can be
            # re-executed
            self._waiting_future = futures.Future()
            raise

        if result == NULL:
            next_state = st.create_state(
                self.process, ProcessState.RUNNING, process=self.process, run_fn=self.done_callback
            )
        else:
            next_state = st.create_state(
                self.process, ProcessState.RUNNING, process=self.process, done_callback=self.done_callback, *result
            )

        return next_state

    def resume(self, value: Any = NULL) -> None:
        assert self._waiting_future is not None, 'Not yet waiting'

        if self._waiting_future.done():
            return

        self._waiting_future.set_result(value)

    def enter(self) -> None: ...

    def exit(self) -> None: ...


@final
class Excepted(persistence.Savable):
    """
    Excepted state, can optionally provide exception and traceback

    :param exception: The exception instance
    :param traceback: An optional exception traceback
    """

    LABEL: ClassVar = ProcessState.EXCEPTED
    ALLOWED: ClassVar[set[str]] = set()

    EXC_VALUE = 'ex_value'
    TRACEBACK = 'traceback'

    is_terminal: ClassVar = True

    def __init__(
        self,
        exception: Optional[BaseException],
        traceback: Optional[TracebackType] = None,
    ):
        """
        :param exception: The exception instance
        :param traceback: An optional exception traceback
        """
        self.exception = exception
        self.traceback = traceback

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
                self.traceback = tblib.Traceback.from_string(saved_state[self.TRACEBACK], strict=False)
            except KeyError:
                self.traceback = None
        else:
            self.traceback = None

    def get_exc_info(
        self,
    ) -> Tuple[Optional[Type[BaseException]], Optional[BaseException], Optional[TracebackType]]:
        """
        Recreate the exc_info tuple and return it
        """
        return (
            type(self.exception) if self.exception else None,
            self.exception,
            self.traceback,
        )

    def enter(self) -> None: ...

    def exit(self) -> None: ...


@final
@auto_persist('result', 'successful')
class Finished(persistence.Savable):
    """State for process is finished.

    :param result: The result of process
    :param successful: Boolean for the exit code is ``0`` the process is successful.
    """

    LABEL: ClassVar = ProcessState.FINISHED
    ALLOWED: ClassVar[set[str]] = set()

    is_terminal: ClassVar[bool] = True

    def __init__(self, result: Any, successful: bool) -> None:
        self.result = result
        self.successful = successful

    def load_instance_state(self, saved_state: SAVED_STATE_TYPE, load_context: persistence.LoadSaveContext) -> None:
        super().load_instance_state(saved_state, load_context)

    def enter(self) -> None: ...

    def exit(self) -> None: ...


@final
@auto_persist('msg')
class Killed(persistence.Savable):
    """
    Represents a state where a process has been killed.

    This state is used to indicate that a process has been terminated and can optionally
    include a message providing details about the termination.

    :param msg: An optional message explaining the reason for the process termination.
    """

    LABEL: ClassVar = ProcessState.KILLED
    ALLOWED: ClassVar[set[str]] = set()

    is_terminal: ClassVar[bool] = True

    def __init__(self, msg: Optional[MessageType]):
        """
        :param msg: Optional kill message
        """
        self.msg = msg

    def load_instance_state(self, saved_state: SAVED_STATE_TYPE, load_context: persistence.LoadSaveContext) -> None:
        super().load_instance_state(saved_state, load_context)

    def enter(self) -> None: ...

    def exit(self) -> None: ...


# endregion
