# -*- coding: utf-8 -*-

import sys
import traceback
from collections.abc import Awaitable
from enum import Enum
from types import TracebackType
from typing import (
    Any,
    Callable,
    ClassVar,
    cast,
    final,
)

import yaml
from typing_extensions import Self, override

from plumpy.loaders import ObjectLoader
from plumpy.message import MessageBuilder, MessageType
from plumpy.persistence import ensure_object_loader

try:
    import tblib

    _HAS_TBLIB: bool = True
except ImportError:
    _HAS_TBLIB = False

from . import exceptions, futures, persistence, utils
from .base import state_machine as st
from .lang import NULL
from .persistence import (
    LoadSaveContext,
    auto_load,
    auto_persist,
    auto_save,
)
from .utils import SAVED_STATE_TYPE, ensure_coroutine

__all__ = [
    'Continue',
    'Created',
    'Excepted',
    'Finished',
    'Interruption',
    # Commands
    'Kill',
    'KillInterruption',
    'Killed',
    'PauseInterruption',
    'ProcessState',
    'Running',
    'Stop',
    'Wait',
    'Waiting',
]


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


class Command:
    @classmethod
    def recreate_from(cls, saved_state: SAVED_STATE_TYPE, load_context: LoadSaveContext | None = None) -> Self:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = auto_load(cls, saved_state, load_context)
        return obj

    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = auto_save(self, loader)

        return out_state


@auto_persist('msg')
class Kill(Command):
    def __init__(self, msg: MessageType | None = None):
        super().__init__()
        self.msg = msg


class Pause(Command):
    pass


@auto_persist('msg', 'data')
class Wait(Command):
    def __init__(
        self,
        continue_fn: Callable[..., Any] | None = None,
        msg: Any | None = None,
        data: Any | None = None,
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

    @override
    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = persistence.auto_save(self, loader)
        out_state[self.CONTINUE_FN] = self.continue_fn.__name__

        return out_state

    @override
    @classmethod
    def recreate_from(cls, saved_state: SAVED_STATE_TYPE, load_context: LoadSaveContext | None = None) -> Self:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = auto_load(cls, saved_state, load_context)

        try:
            obj.continue_fn = utils.load_function(saved_state[obj.CONTINUE_FN])
        except ValueError:
            if load_context is not None:
                obj.continue_fn = getattr(load_context.proc, saved_state[obj.CONTINUE_FN])
            else:
                raise
        return obj


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


@final
@auto_persist('args', 'kwargs')
class Created:
    LABEL: ClassVar = ProcessState.CREATED
    ALLOWED: ClassVar = {ProcessState.RUNNING, ProcessState.KILLED, ProcessState.EXCEPTED}

    RUN_FN = 'run_fn'
    is_terminal: ClassVar[bool] = False

    def __init__(self, process: 'st.StateMachine', run_fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        assert run_fn is not None
        self.process = process
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs

    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = auto_save(self, loader)
        out_state[self.RUN_FN] = self.run_fn.__name__

        return out_state

    @classmethod
    def recreate_from(cls, saved_state: SAVED_STATE_TYPE, load_context: LoadSaveContext | None = None) -> Self:
        """
        Recreate a :class:`Created` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context for runtime attributes

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = auto_load(cls, saved_state)
        obj.process = load_context.process
        obj.run_fn = getattr(obj.process, saved_state[obj.RUN_FN])

        return obj

    def execute(self) -> st.State:
        return st.create_state(
            self.process, ProcessState.RUNNING, process=self.process, run_fn=self.run_fn, *self.args, **self.kwargs
        )

    def enter(self) -> None: ...

    def exit(self) -> None: ...


@final
@auto_persist('args', 'kwargs')
class Running:
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
    _command: Command | None = None
    _running: bool = False
    _run_handle = None

    is_terminal: ClassVar[bool] = False

    def __init__(
        self, process: 'st.StateMachine', run_fn: Callable[..., Awaitable[Any] | Any], *args: Any, **kwargs: Any
    ) -> None:
        assert run_fn is not None
        self.process = process
        self.run_fn = ensure_coroutine(run_fn)
        self.args = args
        self.kwargs = kwargs
        self._run_handle = None

    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = auto_save(self, loader)

        out_state[self.RUN_FN] = self.run_fn.__name__
        if self._command is not None:
            out_state[self.COMMAND] = self._command.save()

        return out_state

    @classmethod
    def recreate_from(cls, saved_state: SAVED_STATE_TYPE, load_context: LoadSaveContext | None = None) -> Self:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = auto_load(cls, saved_state, load_context)
        obj.process = load_context.process

        obj.run_fn = ensure_coroutine(getattr(obj.process, saved_state[obj.RUN_FN]))
        if obj.COMMAND in saved_state:
            loaded_cmd = persistence.load(saved_state[obj.COMMAND], load_context)
            if not isinstance(loaded_cmd, Command):
                # runtime check for loading from persistence
                # XXX: debug log in principle unreachable
                raise RuntimeError(f'command `{obj.COMMAND}` loaded from Running state not a valid `Command` type')

            obj._command = loaded_cmd

        return obj

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

    def _action_command(self, command: Command) -> st.State:
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
class Waiting:
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
        process: 'st.StateMachine',
        done_callback: Callable[..., Any] | None,
        msg: str | None = None,
        data: Any | None = None,
    ) -> None:
        self.process = process
        self.done_callback = done_callback
        self.msg = msg
        self.data = data
        self._waiting_future: futures.Future = futures.Future()

    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = auto_save(self, loader)

        if self.done_callback is not None:
            out_state[self.DONE_CALLBACK] = self.done_callback.__name__

        return out_state

    @classmethod
    def recreate_from(cls, saved_state: SAVED_STATE_TYPE, load_context: LoadSaveContext | None = None) -> Self:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = auto_load(cls, saved_state, load_context)
        obj.process = load_context.process

        callback_name = saved_state.get(obj.DONE_CALLBACK, None)
        if callback_name is not None:
            obj.done_callback = getattr(obj.process, callback_name)
        else:
            obj.done_callback = None
        obj._waiting_future = futures.Future()
        return obj

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
@auto_persist()
class Excepted:
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
        exception: BaseException | None,
        traceback: TracebackType | None = None,
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

    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = auto_save(self, loader)

        out_state[self.EXC_VALUE] = yaml.dump(self.exception)
        if self.traceback is not None:
            out_state[self.TRACEBACK] = ''.join(traceback.format_tb(self.traceback))

        return out_state

    @classmethod
    def recreate_from(cls, saved_state: SAVED_STATE_TYPE, load_context: LoadSaveContext | None = None) -> Self:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = auto_load(cls, saved_state, load_context)

        obj.exception = yaml.load(saved_state[obj.EXC_VALUE], Loader=yaml.UnsafeLoader)
        if _HAS_TBLIB:
            try:
                obj.traceback = tblib.Traceback.from_string(saved_state[obj.TRACEBACK], strict=False)
            except KeyError:
                obj.traceback = None
        else:
            obj.traceback = None
        return obj

    def get_exc_info(
        self,
    ) -> tuple[type[BaseException] | None, BaseException | None, TracebackType | None]:
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
class Finished:
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

    @classmethod
    def recreate_from(cls, saved_state: SAVED_STATE_TYPE, load_context: LoadSaveContext | None = None) -> Self:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = auto_load(cls, saved_state, load_context)
        return obj

    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = auto_save(self, loader)

        return out_state

    def enter(self) -> None: ...

    def exit(self) -> None: ...


@final
@auto_persist('msg')
class Killed:
    """
    Represents a state where a process has been killed.

    This state is used to indicate that a process has been terminated and can optionally
    include a message providing details about the termination.

    :param msg: An optional message explaining the reason for the process termination.
    """

    LABEL: ClassVar = ProcessState.KILLED
    ALLOWED: ClassVar[set[str]] = set()

    is_terminal: ClassVar[bool] = True

    def __init__(self, msg: MessageType | None):
        """
        :param msg: Optional kill message
        """
        self.msg = msg

    @classmethod
    def recreate_from(cls, saved_state: SAVED_STATE_TYPE, load_context: LoadSaveContext | None = None) -> Self:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = auto_load(cls, saved_state, load_context)
        return obj

    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = auto_save(self, loader)

        return out_state

    def enter(self) -> None: ...

    def exit(self) -> None: ...


# endregion
