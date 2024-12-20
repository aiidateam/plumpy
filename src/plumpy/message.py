# -*- coding: utf-8 -*-
"""Module for process level coordination functions and classes"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Union, cast

from plumpy.coordinator import Coordinator
from plumpy.exceptions import PersistenceError, TaskRejectedError

from plumpy.exceptions import PersistenceError, TaskRejectedError

from . import loaders, persistence
from .utils import PID_TYPE

__all__ = [
    'MessageBuilder',
    'ProcessLauncher',
    'create_continue_body',
    'create_launch_body',
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

MessageType = Dict[str, Any]


class MessageBuilder:
    """MessageBuilder will construct different messages that can passing over coordinator."""

    @classmethod
    def play(cls, text: str | None = None) -> MessageType:
        """The play message send over coordinator."""
        return {
            INTENT_KEY: Intent.PLAY,
            MESSAGE_TEXT_KEY: text,
        }

    @classmethod
    def pause(cls, text: str | None = None) -> MessageType:
        """The pause message send over coordinator."""
        return {
            INTENT_KEY: Intent.PAUSE,
            MESSAGE_TEXT_KEY: text,
        }

    @classmethod
    def kill(cls, text: str | None = None, force_kill: bool = False) -> MessageType:
        """The kill message send over coordinator."""
        return {
            INTENT_KEY: Intent.KILL,
            MESSAGE_TEXT_KEY: text,
            FORCE_KILL_KEY: force_kill,
        }

    @classmethod
    def status(cls, text: str | None = None) -> MessageType:
        """The status message send over coordinator."""
        return {
            INTENT_KEY: Intent.STATUS,
            MESSAGE_TEXT_KEY: text,
        }


def create_launch_body(
    process_class: str,
    init_args: Optional[Sequence[Any]] = None,
    init_kwargs: Optional[Dict[str, Any]] = None,
    persist: bool = False,
    loader: Optional[loaders.ObjectLoader] = None,
    nowait: bool = True,
) -> Dict[str, Any]:
    """
    Create a message body for the launch action

    :param process_class: the class of the process to launch
    :param init_args: any initialisation positional arguments
    :param init_kwargs: any initialisation keyword arguments
    :param persist: persist this process if True, otherwise don't
    :param loader: the loader to use to load the persisted process
    :param nowait: wait for the process to finish before completing the task, otherwise just return the PID
    :return: a dictionary with the body of the message to launch the process
    :rtype: dict
    """
    if loader is None:
        loader = loaders.get_object_loader()

    msg_body = {
        TASK_KEY: LAUNCH_TASK,
        TASK_ARGS: {
            PROCESS_CLASS_KEY: loader.identify_object(process_class),
            PERSIST_KEY: persist,
            NOWAIT_KEY: nowait,
            ARGS_KEY: init_args,
            KWARGS_KEY: init_kwargs,
        },
    }
    return msg_body


def create_continue_body(pid: 'PID_TYPE', tag: Optional[str] = None, nowait: bool = False) -> Dict[str, Any]:
    """
    Create a message body to continue an existing process
    :param pid: the pid of the existing process
    :param tag: the optional persistence tag
    :param nowait: wait for the process to finish before completing the task, otherwise just return the PID
    :return: a dictionary with the body of the message to continue the process

    """
    msg_body = {TASK_KEY: CONTINUE_TASK, TASK_ARGS: {PID_KEY: pid, NOWAIT_KEY: nowait, TAG_KEY: tag}}
    return msg_body


def create_create_body(
    process_class: str,
    init_args: Optional[Sequence[Any]] = None,
    init_kwargs: Optional[Dict[str, Any]] = None,
    persist: bool = False,
    loader: Optional[loaders.ObjectLoader] = None,
) -> Dict[str, Any]:
    """
    Create a message body to create a new process
    :param process_class: the class of the process to launch
    :param init_args: any initialisation positional arguments
    :param init_kwargs: any initialisation keyword arguments
    :param persist: persist this process if True, otherwise don't
    :param loader: the loader to use to load the persisted process
    :return: a dictionary with the body of the message to launch the process

    """
    if loader is None:
        loader = loaders.get_object_loader()

    msg_body = {
        TASK_KEY: CREATE_TASK,
        TASK_ARGS: {
            PROCESS_CLASS_KEY: loader.identify_object(process_class),
            PERSIST_KEY: persist,
            ARGS_KEY: init_args,
            KWARGS_KEY: init_kwargs,
        },
    }
    return msg_body


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
        loop: Optional[asyncio.AbstractEventLoop] = None,
        persister: Optional[persistence.Persister] = None,
        load_context: Optional[persistence.LoadSaveContext] = None,
        loader: Optional[loaders.ObjectLoader] = None,
    ) -> None:
        self._loop = loop
        self._persister = persister
        self._load_context = load_context if load_context is not None else persistence.LoadSaveContext()

        if loader is not None:
            self._loader = loader
            self._load_context = self._load_context.copyextend(loader=loader)
        else:
            self._loader = loaders.get_object_loader()

    async def __call__(self, coordinator: Coordinator, task: Dict[str, Any]) -> Union[PID_TYPE, Any]:
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
        init_args: Optional[Sequence[Any]] = None,
        init_kwargs: Optional[Dict[str, Any]] = None,
    ) -> Union[PID_TYPE, Any]:
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

    async def _continue(self, pid: 'PID_TYPE', nowait: bool, tag: Optional[str] = None) -> Union[PID_TYPE, Any]:
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
        init_args: Optional[Sequence[Any]] = None,
        init_kwargs: Optional[Dict[str, Any]] = None,
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
