# -*- coding: utf-8 -*-
"""Module for process level coordination functions and classes"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

from plumpy.coordinator import Coordinator
from plumpy.exceptions import PersistenceError, TaskRejectedError

from . import loaders, persistence
from .utils import PID_TYPE

__all__ = [
    'Message',
    'MsgContinue',
    'MsgCreate',
    'MsgKill',
    'MsgLaunch',
    'MsgPause',
    'MsgPlay',
    'MsgStatus',
    'ProcessLauncher',
]

if TYPE_CHECKING:
    from .processes import Process

INTENT_KEY = 'intent'
MESSAGE_TEXT_KEY = 'message'
FORCE_KILL_KEY = 'force_kill'


class Intent:
    """Intent constants for a process message"""

    PLAY: str = 'play'
    PAUSE: str = 'pause'
    KILL: str = 'kill'
    STATUS: str = 'status'


TASK_KEY = 'task'
TASK_ARGS = 'args'
PERSIST_KEY = 'persist'
# Launch
PROCESS_CLASS_KEY = 'process_class'
ARGS_KEY = 'init_args'
KWARGS_KEY = 'init_kwargs'
NOWAIT_KEY = 'nowait'
# Continue
PID_KEY = 'pid'
TAG_KEY = 'tag'
# Task types
LAUNCH_TASK = 'launch'
CONTINUE_TASK = 'continue'
CREATE_TASK = 'create'

LOGGER = logging.getLogger(__name__)

Message = dict[str, Any]


class MsgPlay:
    @classmethod
    def new(cls, text: str | None = None) -> Message:
        """The play message send over coordinator."""
        return {
            INTENT_KEY: Intent.PLAY,
            MESSAGE_TEXT_KEY: text,
        }


class MsgPause:
    """
    The 'pause' message sent over a coordinator.
    """

    @classmethod
    def new(cls, text: str | None = None) -> Message:
        return {
            INTENT_KEY: Intent.PAUSE,
            MESSAGE_TEXT_KEY: text,
        }


class MsgKill:
    """
    The 'kill' message sent over a coordinator.
    """

    @classmethod
    def new(cls, text: str | None = None, force_kill: bool = False) -> Message:
        return {
            INTENT_KEY: Intent.KILL,
            MESSAGE_TEXT_KEY: text,
            FORCE_KILL_KEY: force_kill,
        }


class MsgStatus:
    """
    The 'status' message sent over a coordinator.
    """

    @classmethod
    def new(cls, text: str | None = None) -> Message:
        return {
            INTENT_KEY: Intent.STATUS,
            MESSAGE_TEXT_KEY: text,
        }


class MsgLaunch:
    """
    Create the message payload for the launch action.
    """

    @classmethod
    def new(
        cls,
        process_class: str,
        init_args: Sequence[Any] | None = None,
        init_kwargs: dict[str, Any] | None = None,
        persist: bool = False,
        loader: 'loaders.ObjectLoader | None' = None,
        nowait: bool = True,
    ) -> dict[str, Any]:
        """
        Create a message body for the launch action
        """
        if loader is None:
            loader = loaders.get_object_loader()

        return {
            TASK_KEY: LAUNCH_TASK,
            TASK_ARGS: {
                PROCESS_CLASS_KEY: loader.identify_object(process_class),
                PERSIST_KEY: persist,
                NOWAIT_KEY: nowait,
                ARGS_KEY: init_args,
                KWARGS_KEY: init_kwargs,
            },
        }


class MsgContinue:
    """
    Create the message payload to continue an existing process.
    """

    @classmethod
    def new(
        cls,
        pid: 'PID_TYPE',
        tag: str | None = None,
        nowait: bool = False,
    ) -> dict[str, Any]:
        """
        Create a message body to continue an existing process.
        """
        return {
            TASK_KEY: CONTINUE_TASK,
            TASK_ARGS: {
                PID_KEY: pid,
                NOWAIT_KEY: nowait,
                TAG_KEY: tag,
            },
        }


class MsgCreate:
    """
    Create the message payload to create a new process.
    """

    @classmethod
    def new(
        cls,
        process_class: str,
        init_args: Sequence[Any] | None = None,
        init_kwargs: dict[str, Any] | None = None,
        persist: bool = False,
        loader: 'loaders.ObjectLoader | None' = None,
    ) -> dict[str, Any]:
        """
        Create a message body to create a new process.
        """
        if loader is None:
            loader = loaders.get_object_loader()

        return {
            TASK_KEY: CREATE_TASK,
            TASK_ARGS: {
                PROCESS_CLASS_KEY: loader.identify_object(process_class),
                PERSIST_KEY: persist,
                ARGS_KEY: init_args,
                KWARGS_KEY: init_kwargs,
            },
        }


class ProcessLauncher:
    """
    Takes incoming task messages and uses them to launch processes.

    Expected format of task:

    For launch::

        {
            'task': <LAUNCH_TASK>
            'process_class': <Process class to launch>
            'args': <tuple of positional args for process constructor>
            'kwargs': <dict of keyword args for process constructor>.
            'nowait': True or False
        }

    For continue::

        {
            'task': <CONTINUE_TASK>
            'pid': <Process ID>
            'nowait': True or False
        }
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop | None = None,
        persister: persistence.Persister | None = None,
        load_context: persistence.LoadSaveContext | None = None,
        loader: loaders.ObjectLoader | None = None,
    ) -> None:
        self._loop = loop
        self._persister = persister
        self._load_context = load_context if load_context is not None else persistence.LoadSaveContext()

        if loader is not None:
            self._loader = loader
            self._load_context = self._load_context.copyextend(loader=loader)
        else:
            self._loader = loaders.get_object_loader()

    async def __call__(self, coordinator: Coordinator, task: dict[str, Any]) -> PID_TYPE | Any:
        """
        Receive a task.
        :param task: The task message
        """
        task_type = task[TASK_KEY]
        if task_type == LAUNCH_TASK:
            return await self._launch(**task.get(TASK_ARGS, {}))
        if task_type == CONTINUE_TASK:
            return await self._continue(**task.get(TASK_ARGS, {}))
        if task_type == CREATE_TASK:
            return await self._create(**task.get(TASK_ARGS, {}))

        raise TaskRejectedError

    async def _launch(
        self,
        process_class: str,
        persist: bool,
        nowait: bool,
        init_args: Sequence[Any] | None = None,
        init_kwargs: dict[str, Any] | None = None,
    ) -> PID_TYPE | Any:
        """
        Launch the process

        :param process_class: the process class to launch
        :param persist: should the process be persisted
        :param nowait: if True only return when the process finishes
        :param init_args: positional arguments to the process constructor
        :param init_kwargs: keyword arguments to the process constructor
        :return: the pid of the created process or the outputs (if nowait=False)
        """
        if persist and not self._persister:
            raise PersistenceError('Cannot persist process, no persister')

        if init_args is None:
            init_args = ()
        if init_kwargs is None:
            init_kwargs = {}

        proc_class = self._loader.load_object(process_class)
        proc = proc_class(*init_args, **init_kwargs)
        if persist and self._persister is not None:
            self._persister.save_checkpoint(proc)

        if nowait:
            # XXX: can return a reference and gracefully use task to cancel itself when the upper call stack fails
            asyncio.ensure_future(proc.step_until_terminated())  # noqa: RUF006
            return proc.pid

        await proc.step_until_terminated()

        return proc.future().result()

    async def _continue(self, pid: 'PID_TYPE', nowait: bool, tag: str | None = None) -> PID_TYPE | Any:
        """
        Continue the process

        :param pid: the pid of the process to continue
        :param nowait: if True don't wait for the process to complete
        :param tag: the checkpoint tag to continue from
        """
        if not self._persister:
            LOGGER.warning('rejecting task: cannot continue process<%d> because no persister is available', pid)
            raise PersistenceError('Cannot continue process, no persister')

        # Do not catch exceptions here, because if these operations fail, the continue task should except and bubble up
        saved_state = self._persister.load_checkpoint(pid, tag)
        proc = cast('Process', saved_state.unbundle(self._load_context))

        if nowait:
            # XXX: can return a reference and gracefully use task to cancel itself when the upper call stack fails
            asyncio.ensure_future(proc.step_until_terminated())  # noqa: RUF006
            return proc.pid

        await proc.step_until_terminated()

        return proc.future().result()

    async def _create(
        self,
        process_class: str,
        persist: bool,
        init_args: Sequence[Any] | None = None,
        init_kwargs: dict[str, Any] | None = None,
    ) -> 'PID_TYPE':
        """
        Create the process

        :param process_class: the process class to create
        :param persist: should the process be persisted
        :param init_args: positional arguments to the process constructor
        :param init_kwargs: keyword arguments to the process constructor
        :return: the pid of the created process
        """
        if persist and not self._persister:
            raise PersistenceError('Cannot persist process, no persister')

        if init_args is None:
            init_args = ()
        if init_kwargs is None:
            init_kwargs = {}

        proc_class = self._loader.load_object(process_class)
        proc = proc_class(*init_args, **init_kwargs)
        if persist and self._persister is not None:
            self._persister.save_checkpoint(proc)

        return proc.pid
