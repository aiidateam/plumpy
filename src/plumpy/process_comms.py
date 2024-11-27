# -*- coding: utf-8 -*-
"""Module for process level communication functions and classes"""

import asyncio
import copy
import logging
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Union, cast

import kiwipy

from . import communications, futures, loaders, persistence
from .utils import PID_TYPE

__all__ = [
    'PAUSE_MSG',
    'PLAY_MSG',
    'KILL_MSG',
    'STATUS_MSG',
    'ProcessLauncher',
    'create_continue_body',
    'create_launch_body',
    'RemoteProcessThreadController',
    'RemoteProcessController',
]

if TYPE_CHECKING:
    from .processes import Process  # pylint: disable=cyclic-import

ProcessResult = Any
ProcessStatus = Any

INTENT_KEY = 'intent'
MESSAGE_KEY = 'message'


class Intent:
    """Intent constants for a process message"""

    # pylint: disable=too-few-public-methods
    PLAY: str = 'play'
    PAUSE: str = 'pause'
    KILL: str = 'kill'
    STATUS: str = 'status'


PAUSE_MSG = {INTENT_KEY: Intent.PAUSE}
PLAY_MSG = {INTENT_KEY: Intent.PLAY}
KILL_MSG = {INTENT_KEY: Intent.KILL}
STATUS_MSG = {INTENT_KEY: Intent.STATUS}

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


class RemoteProcessController:
    """
    Control remote processes using coroutines that will send messages and wait
    (in a non-blocking way) for their response
    """

    def __init__(self, communicator: kiwipy.Communicator) -> None:
        self._communicator = communicator

    async def get_status(self, pid: 'PID_TYPE') -> 'ProcessStatus':
        """
        Get the status of a process with the given PID
        :param pid: the process id
        :return: the status response from the process
        """
        future = self._communicator.rpc_send(pid, STATUS_MSG)
        result = await asyncio.wrap_future(future)
        return result

    async def pause_process(self, pid: 'PID_TYPE', msg: Optional[Any] = None) -> 'ProcessResult':
        """
        Pause the process

        :param pid: the pid of the process to pause
        :param msg: optional pause message
        :return: True if paused, False otherwise
        """
        message = copy.copy(PAUSE_MSG)
        if msg is not None:
            message[MESSAGE_KEY] = msg

        pause_future = self._communicator.rpc_send(pid, message)
        # rpc_send return a thread future from communicator
        future = await asyncio.wrap_future(pause_future)
        # future is just returned from rpc call which return a kiwipy future
        result = await asyncio.wrap_future(future)
        return result

    async def play_process(self, pid: 'PID_TYPE') -> 'ProcessResult':
        """
        Play the process

        :param pid: the pid of the process to play
        :return: True if played, False otherwise
        """
        play_future = self._communicator.rpc_send(pid, PLAY_MSG)
        future = await asyncio.wrap_future(play_future)
        result = await asyncio.wrap_future(future)
        return result

    async def kill_process(self, pid: 'PID_TYPE', msg: Optional[Any] = None) -> 'ProcessResult':
        """
        Kill the process

        :param pid: the pid of the process to kill
        :param msg: optional kill message
        :return: True if killed, False otherwise
        """
        message = copy.copy(KILL_MSG)
        if msg is not None:
            message[MESSAGE_KEY] = msg

        # Wait for the communication to go through
        kill_future = self._communicator.rpc_send(pid, message)
        future = await asyncio.wrap_future(kill_future)
        # Now wait for the kill to be enacted
        result = await asyncio.wrap_future(future)
        return result

    async def continue_process(
        self, pid: 'PID_TYPE', tag: Optional[str] = None, nowait: bool = False, no_reply: bool = False
    ) -> Optional['ProcessResult']:
        """
        Continue the process

        :param _communicator: the communicator
        :param pid: the pid of the process to continue
        :param tag: the checkpoint tag to continue from
        """
        message = create_continue_body(pid=pid, tag=tag, nowait=nowait)
        # Wait for the communication to go through
        continue_future = self._communicator.task_send(message, no_reply=no_reply)
        future = await asyncio.wrap_future(continue_future)

        if no_reply:
            return None

        # Now wait for the result of the task
        result = await asyncio.wrap_future(future)
        return result

    async def launch_process(
        self,
        process_class: str,
        init_args: Optional[Sequence[Any]] = None,
        init_kwargs: Optional[Dict[str, Any]] = None,
        persist: bool = False,
        loader: Optional[loaders.ObjectLoader] = None,
        nowait: bool = False,
        no_reply: bool = False,
    ) -> 'ProcessResult':
        """
        Launch a process given the class and constructor arguments

        :param process_class: the class of the process to launch
        :param init_args: the constructor positional arguments
        :param init_kwargs: the constructor keyword arguments
        :param persist: should the process be persisted
        :param loader: the classloader to use
        :param nowait: if True, don't wait for the process to send a response, just return the pid
        :param no_reply: if True, this call will be fire-and-forget, i.e. no return value
        :return: the result of launching the process
        """
        # pylint: disable=too-many-arguments
        message = create_launch_body(process_class, init_args, init_kwargs, persist, loader, nowait)
        launch_future = self._communicator.task_send(message, no_reply=no_reply)
        future = await asyncio.wrap_future(launch_future)

        if no_reply:
            return

        result = await asyncio.wrap_future(future)
        return result

    async def execute_process(
        self,
        process_class: str,
        init_args: Optional[Sequence[Any]] = None,
        init_kwargs: Optional[Dict[str, Any]] = None,
        loader: Optional[loaders.ObjectLoader] = None,
        nowait: bool = False,
        no_reply: bool = False,
    ) -> 'ProcessResult':
        """
        Execute a process.  This call will first send a create task and then a continue task over
        the communicator.  This means that if communicator messages are durable then the process
        will run until the end even if this interpreter instance ceases to exist.

        :param process_class: the process class to execute
        :param init_args: the positional arguments to the class constructor
        :param init_kwargs: the keyword arguments to the class constructor
        :param loader: the class loader to use
        :param nowait: if True, don't wait for the process to send a response
        :param no_reply: if True, this call will be fire-and-forget, i.e. no return value
        :return: the result of executing the process
        """
        # pylint: disable=too-many-arguments
        message = create_create_body(process_class, init_args, init_kwargs, persist=True, loader=loader)

        create_future = self._communicator.task_send(message)
        future = await asyncio.wrap_future(create_future)
        pid: 'PID_TYPE' = await asyncio.wrap_future(future)

        message = create_continue_body(pid, nowait=nowait)
        continue_future = self._communicator.task_send(message, no_reply=no_reply)
        future = await asyncio.wrap_future(continue_future)

        if no_reply:
            return

        result = await asyncio.wrap_future(future)
        return result


class RemoteProcessThreadController:
    """
    A class that can be used to control and launch remote processes
    """

    def __init__(self, communicator: kiwipy.Communicator):
        """
        Create a new process controller

        :param communicator: the communicator to use

        """
        self._communicator = communicator

    def get_status(self, pid: 'PID_TYPE') -> kiwipy.Future:
        """Get the status of a process with the given PID.

        :param pid: the process id
        :return: the status response from the process
        """
        return self._communicator.rpc_send(pid, STATUS_MSG)

    def pause_process(self, pid: 'PID_TYPE', msg: Optional[Any] = None) -> kiwipy.Future:
        """
        Pause the process

        :param pid: the pid of the process to pause
        :param msg: optional pause message
        :return: a response future from the process to be paused

        """
        message = copy.copy(PAUSE_MSG)
        if msg is not None:
            message[MESSAGE_KEY] = msg

        return self._communicator.rpc_send(pid, message)

    def pause_all(self, msg: Any) -> None:
        """
        Pause all processes that are subscribed to the same communicator

        :param msg: an optional pause message
        """
        self._communicator.broadcast_send(msg, subject=Intent.PAUSE)

    def play_process(self, pid: 'PID_TYPE') -> kiwipy.Future:
        """
        Play the process

        :param pid: the pid of the process to pause
        :return: a response future from the process to be played

        """
        return self._communicator.rpc_send(pid, PLAY_MSG)

    def play_all(self) -> None:
        """
        Play all processes that are subscribed to the same communicator
        """
        self._communicator.broadcast_send(None, subject=Intent.PLAY)

    def kill_process(self, pid: 'PID_TYPE', msg: Optional[Any] = None) -> kiwipy.Future:
        """
        Kill the process

        :param pid: the pid of the process to kill
        :param msg: optional kill message
        :return: a response future from the process to be killed

        """
        message = copy.copy(KILL_MSG)
        if msg is not None:
            message[MESSAGE_KEY] = msg

        return self._communicator.rpc_send(pid, message)

    def kill_all(self, msg: Optional[Any]) -> None:
        """
        Kill all processes that are subscribed to the same communicator

        :param msg: an optional pause message
        """
        self._communicator.broadcast_send(msg, subject=Intent.KILL)

    def continue_process(
        self, pid: 'PID_TYPE', tag: Optional[str] = None, nowait: bool = False, no_reply: bool = False
    ) -> Union[None, PID_TYPE, ProcessResult]:
        message = create_continue_body(pid=pid, tag=tag, nowait=nowait)
        return self.task_send(message, no_reply=no_reply)

    def launch_process(
        self,
        process_class: str,
        init_args: Optional[Sequence[Any]] = None,
        init_kwargs: Optional[Dict[str, Any]] = None,
        persist: bool = False,
        loader: Optional[loaders.ObjectLoader] = None,
        nowait: bool = False,
        no_reply: bool = False,
    ) -> Union[None, PID_TYPE, ProcessResult]:
        # pylint: disable=too-many-arguments
        """
        Launch the process

        :param process_class: the process class to launch
        :param init_args: positional arguments to the process constructor
        :param init_kwargs: keyword arguments to the process constructor
        :param persist: should the process be persisted
        :param loader: the class loader to use
        :param nowait: if True only return when the process finishes
        :param no_reply: don't send a reply to the sender
        :return: the pid of the created process or the outputs (if nowait=False)
        """
        message = create_launch_body(process_class, init_args, init_kwargs, persist, loader, nowait)
        return self.task_send(message, no_reply=no_reply)

    def execute_process(
        self,
        process_class: str,
        init_args: Optional[Sequence[Any]] = None,
        init_kwargs: Optional[Dict[str, Any]] = None,
        loader: Optional[loaders.ObjectLoader] = None,
        nowait: bool = False,
        no_reply: bool = False,
    ) -> Union[None, PID_TYPE, ProcessResult]:
        """
        Execute a process.  This call will first send a create task and then a continue task over
        the communicator.  This means that if communicator messages are durable then the process
        will run until the end even if this interpreter instance ceases to exist.

        :param process_class: the process class to execute
        :param init_args: the positional arguments to the class constructor
        :param init_kwargs: the keyword arguments to the class constructor
        :param loader: the class loader to use
        :param nowait: if True, don't wait for the process to send a response
        :param no_reply: if True, this call will be fire-and-forget, i.e. no return value
        :return: the result of executing the process
        """
        # pylint: disable=too-many-arguments
        message = create_create_body(process_class, init_args, init_kwargs, persist=True, loader=loader)

        execute_future = kiwipy.Future()
        create_future = futures.unwrap_kiwi_future(self._communicator.task_send(message))

        def on_created(_: Any) -> None:
            with kiwipy.capture_exceptions(execute_future):
                pid: 'PID_TYPE' = create_future.result()
                continue_future = self.continue_process(pid, nowait=nowait, no_reply=no_reply)
                kiwipy.chain(continue_future, execute_future)

        create_future.add_done_callback(on_created)
        return execute_future

    def task_send(self, message: Any, no_reply: bool = False) -> Optional[Any]:
        """
        Send a task to be performed using the communicator

        :param message: the task message
        :param no_reply: if True, this call will be fire-and-forget, i.e. no return value
        :return: the response from the remote side (if no_reply=False)
        """
        return self._communicator.task_send(message, no_reply=no_reply)


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

    async def __call__(self, communicator: kiwipy.Communicator, task: Dict[str, Any]) -> Union[PID_TYPE, ProcessResult]:
        """
        Receive a task.
        :param task: The task message
        """
        task_type = task[TASK_KEY]
        if task_type == LAUNCH_TASK:
            return await self._launch(communicator, **task.get(TASK_ARGS, {}))
        if task_type == CONTINUE_TASK:
            return await self._continue(communicator, **task.get(TASK_ARGS, {}))
        if task_type == CREATE_TASK:
            return await self._create(communicator, **task.get(TASK_ARGS, {}))

        raise communications.TaskRejected

    async def _launch(
        self,
        _communicator: kiwipy.Communicator,
        process_class: str,
        persist: bool,
        nowait: bool,
        init_args: Optional[Sequence[Any]] = None,
        init_kwargs: Optional[Dict[str, Any]] = None,
    ) -> Union[PID_TYPE, ProcessResult]:
        """
        Launch the process

        :param _communicator: the communicator
        :param process_class: the process class to launch
        :param persist: should the process be persisted
        :param nowait: if True only return when the process finishes
        :param init_args: positional arguments to the process constructor
        :param init_kwargs: keyword arguments to the process constructor
        :return: the pid of the created process or the outputs (if nowait=False)
        """
        if persist and not self._persister:
            raise communications.TaskRejected('Cannot persist process, no persister')

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

    async def _continue(
        self, _communicator: kiwipy.Communicator, pid: 'PID_TYPE', nowait: bool, tag: Optional[str] = None
    ) -> Union[PID_TYPE, ProcessResult]:
        """
        Continue the process

        :param _communicator: the communicator
        :param pid: the pid of the process to continue
        :param nowait: if True don't wait for the process to complete
        :param tag: the checkpoint tag to continue from
        """
        if not self._persister:
            LOGGER.warning('rejecting task: cannot continue process<%d> because no persister is available', pid)
            raise communications.TaskRejected('Cannot continue process, no persister')

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
        _communicator: kiwipy.Communicator,
        process_class: str,
        persist: bool,
        init_args: Optional[Sequence[Any]] = None,
        init_kwargs: Optional[Dict[str, Any]] = None,
    ) -> 'PID_TYPE':
        """
        Create the process

        :param _communicator: the communicator
        :param process_class: the process class to create
        :param persist: should the process be persisted
        :param init_args: positional arguments to the process constructor
        :param init_kwargs: keyword arguments to the process constructor
        :return: the pid of the created process
        """
        if persist and not self._persister:
            raise communications.TaskRejected('Cannot persist process, no persister')

        if init_args is None:
            init_args = ()
        if init_kwargs is None:
            init_kwargs = {}

        proc_class = self._loader.load_object(process_class)
        proc = proc_class(*init_args, **init_kwargs)
        if persist and self._persister is not None:
            self._persister.save_checkpoint(proc)

        return proc.pid
