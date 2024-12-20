# -*- coding: utf-8 -*-
"""Module for process level communication functions and classes"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, Hashable, Optional, Sequence, Union

import kiwipy

from plumpy import loaders
from plumpy.coordinator import Coordinator
from plumpy.message import (
    Intent,
    MessageBuilder,
    MessageType,
    create_continue_body,
    create_create_body,
    create_launch_body,
)
from plumpy.utils import PID_TYPE

__all__ = [
    'RemoteProcessController',
    'RemoteProcessThreadController',
]

ProcessResult = Any
ProcessStatus = Any


# FIXME: the class not fit typing of ProcessController protocol
class RemoteProcessController:
    """
    Control remote processes using coroutines that will send messages and wait
    (in a non-blocking way) for their response
    """

    def __init__(self, coordinator: Coordinator) -> None:
        self._coordinator = coordinator

    async def get_status(self, pid: 'PID_TYPE') -> 'ProcessStatus':
        """
        Get the status of a process with the given PID
        :param pid: the process id
        :return: the status response from the process
        """
        future = self._coordinator.rpc_send(pid, MessageBuilder.status())
        result = await asyncio.wrap_future(future)
        return result

    async def pause_process(self, pid: 'PID_TYPE', msg_text: Optional[str] = None) -> 'ProcessResult':
        """
        Pause the process

        :param pid: the pid of the process to pause
        :param msg: optional pause message
        :return: True if paused, False otherwise
        """
        msg = MessageBuilder.pause(text=msg_text)

        pause_future = self._coordinator.rpc_send(pid, msg)
        # rpc_send return a thread future from coordinator
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
        play_future = self._coordinator.rpc_send(pid, MessageBuilder.play())
        future = await asyncio.wrap_future(play_future)
        result = await asyncio.wrap_future(future)
        return result

    async def kill_process(self, pid: 'PID_TYPE', msg_text: Optional[str] = None) -> 'ProcessResult':
        """
        Kill the process

        :param pid: the pid of the process to kill
        :param msg: optional kill message
        :return: True if killed, False otherwise
        """
        msg = MessageBuilder.kill(text=msg_text)

        # Wait for the communication to go through
        kill_future = self._coordinator.rpc_send(pid, msg)
        future = await asyncio.wrap_future(kill_future)
        # Now wait for the kill to be enacted
        result = await asyncio.wrap_future(future)
        return result

    async def continue_process(
        self, pid: 'PID_TYPE', tag: Optional[str] = None, nowait: bool = False, no_reply: bool = False
    ) -> Optional['ProcessResult']:
        """
        Continue the process

        :param _coordinator: the coordinator
        :param pid: the pid of the process to continue
        :param tag: the checkpoint tag to continue from
        """
        message = create_continue_body(pid=pid, tag=tag, nowait=nowait)
        # Wait for the communication to go through
        continue_future = self._coordinator.task_send(message, no_reply=no_reply)
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

        message = create_launch_body(process_class, init_args, init_kwargs, persist, loader, nowait)
        launch_future = self._coordinator.task_send(message, no_reply=no_reply)
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
        the coordinator.  This means that if coordinator messages are durable then the process
        will run until the end even if this interpreter instance ceases to exist.

        :param process_class: the process class to execute
        :param init_args: the positional arguments to the class constructor
        :param init_kwargs: the keyword arguments to the class constructor
        :param loader: the class loader to use
        :param nowait: if True, don't wait for the process to send a response
        :param no_reply: if True, this call will be fire-and-forget, i.e. no return value
        :return: the result of executing the process
        """

        message = create_create_body(process_class, init_args, init_kwargs, persist=True, loader=loader)

        create_future = self._coordinator.task_send(message)
        future = await asyncio.wrap_future(create_future)
        pid: 'PID_TYPE' = await asyncio.wrap_future(future)

        message = create_continue_body(pid, nowait=nowait)
        continue_future = self._coordinator.task_send(message, no_reply=no_reply)
        future = await asyncio.wrap_future(continue_future)

        if no_reply:
            return

        result = await asyncio.wrap_future(future)
        return result


# FIXME: the class not fit typing of ProcessController protocol
class RemoteProcessThreadController:
    """
    A class that can be used to control and launch remote processes
    """

    def __init__(self, coordinator: Coordinator):
        """
        Create a new process controller

        :param coordinator: the coordinator to use

        """
        self._coordinator = coordinator

    def get_status(self, pid: 'PID_TYPE') -> kiwipy.Future:
        """Get the status of a process with the given PID.

        :param pid: the process id
        :return: the status response from the process
        """
        return self._coordinator.rpc_send(pid, MessageBuilder.status())

    def pause_process(self, pid: 'PID_TYPE', msg_text: Optional[str] = None) -> kiwipy.Future:
        """
        Pause the process

        :param pid: the pid of the process to pause
        :param msg: optional pause message
        :return: a response future from the process to be paused

        """
        msg = MessageBuilder.pause(text=msg_text)

        return self._coordinator.rpc_send(pid, msg)

    def pause_all(self, msg_text: Optional[str]) -> None:
        """
        Pause all processes that are subscribed to the same coordinator

        :param msg: an optional pause message
        """
        msg = MessageBuilder.pause(text=msg_text)
        self._coordinator.broadcast_send(msg, subject=Intent.PAUSE)

    def play_process(self, pid: 'PID_TYPE') -> kiwipy.Future:
        """
        Play the process

        :param pid: the pid of the process to pause
        :return: a response future from the process to be played

        """
        return self._coordinator.rpc_send(pid, MessageBuilder.play())

    def play_all(self) -> None:
        """
        Play all processes that are subscribed to the same coordinator
        """
        self._coordinator.broadcast_send(None, subject=Intent.PLAY)

    def kill_process(self, pid: 'PID_TYPE', msg_text: Optional[str] = None) -> kiwipy.Future:
        """
        Kill the process

        :param pid: the pid of the process to kill
        :param msg: optional kill message
        :return: a response future from the process to be killed
        """
        msg = MessageBuilder.kill(text=msg_text)
        return self._coordinator.rpc_send(pid, msg)

    def kill_all(self, msg_text: Optional[str]) -> None:
        """
        Kill all processes that are subscribed to the same coordinator

        :param msg: an optional pause message
        """
        msg = MessageBuilder.kill(msg_text)

        self._coordinator.broadcast_send(msg, subject=Intent.KILL)

    def notify_msg(self, msg: MessageType, sender: Hashable | None = None, subject: str | None = None) -> None:
        """
        Notify all processes by broadcasting of a msg

        :param msg: an optional pause message
        """
        self._coordinator.broadcast_send(msg, sender=sender, subject=subject)

    def continue_process(
        self, pid: 'PID_TYPE', tag: Optional[str] = None, nowait: bool = False, no_reply: bool = False
    ) -> Union[None, PID_TYPE, ProcessResult]:
        message = create_continue_body(pid=pid, tag=tag, nowait=nowait)
        return self._coordinator.task_send(message, no_reply=no_reply)

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
        return self._coordinator.task_send(message, no_reply=no_reply)

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
        the coordinator.  This means that if coordinator messages are durable then the process
        will run until the end even if this interpreter instance ceases to exist.

        :param process_class: the process class to execute
        :param init_args: the positional arguments to the class constructor
        :param init_kwargs: the keyword arguments to the class constructor
        :param loader: the class loader to use
        :param nowait: if True, don't wait for the process to send a response
        :param no_reply: if True, this call will be fire-and-forget, i.e. no return value
        :return: the result of executing the process
        """
        message = create_create_body(process_class, init_args, init_kwargs, persist=True, loader=loader)

        execute_future = kiwipy.Future()
        create_future = self._coordinator.task_send(message)

        def on_created(_: Any) -> None:
            with kiwipy.capture_exceptions(execute_future):
                pid: 'PID_TYPE' = create_future.result()
                continue_future = self.continue_process(pid, nowait=nowait, no_reply=no_reply)
                kiwipy.chain(continue_future, execute_future)

        create_future.add_done_callback(on_created)
        return execute_future
