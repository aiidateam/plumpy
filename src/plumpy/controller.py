# -*- coding: utf-8 -*-
from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Hashable, Optional, Protocol, Union, runtime_checkable

from plumpy import loaders
from plumpy.message import MessageType
from plumpy.utils import PID_TYPE

ProcessResult = Any
ProcessStatus = Any


@runtime_checkable
class ProcessController(Protocol):
    """
    Control processes using coroutines that will send messages and wait
    (in a non-blocking way) for their response
    """

    def get_status(self, pid: 'PID_TYPE') -> ProcessStatus:
        """
        Get the status of a process with the given PID
        :param pid: the process id
        :return: the status response from the process
        """
        ...

    def pause_process(self, pid: 'PID_TYPE', msg_text: str | None = None) -> Any:
        """
        Pause the process

        :param pid: the pid of the process to pause
        :param msg: optional pause message
        :return: True if paused, False otherwise
        """
        ...

    def pause_all(self, msg_text: str | None) -> None:
        """Pause all processes that are subscribed to the same coordinator

        :param msg_text: an optional pause message text
        """
        ...

    def play_process(self, pid: 'PID_TYPE') -> ProcessResult:
        """Play the process

        :param pid: the pid of the process to play
        :return: True if played, False otherwise
        """
        ...

    def play_all(self) -> None:
        """Play all processes that are subscribed to the same coordinator"""

    def kill_process(self, pid: 'PID_TYPE', msg_text: str | None = None) -> Any:
        """Kill the process

        :param pid: the pid of the process to kill
        :param msg: optional kill message
        :return: True if killed, False otherwise
        """
        ...

    def kill_all(self, msg_text: Optional[str]) -> None:
        """Kill all processes that are subscribed to the same coordinator

        :param msg: an optional pause message
        """
        ...

    def notify_msg(self, msg: MessageType, sender: Hashable | None = None, subject: str | None = None) -> None:
        """
        Notify all processes by broadcasting of a msg

        :param msg: an optional pause message
        """

    def continue_process(
        self, pid: 'PID_TYPE', tag: Optional[str] = None, nowait: bool = False, no_reply: bool = False
    ) -> Union[None, PID_TYPE, ProcessResult]:
        """Continue the process

        :param _communicator: the communicator
        :param pid: the pid of the process to continue
        :param tag: the checkpoint tag to continue from
        """
        ...

    def launch_process(
        self,
        process_class: str,
        init_args: Optional[Sequence[Any]] = None,
        init_kwargs: Optional[dict[str, Any]] = None,
        persist: bool = False,
        loader: Optional[loaders.ObjectLoader] = None,
        nowait: bool = False,
        no_reply: bool = False,
    ) -> Union[None, PID_TYPE, ProcessResult]:
        """Launch a process given the class and constructor arguments

        :param process_class: the class of the process to launch
        :param init_args: the constructor positional arguments
        :param init_kwargs: the constructor keyword arguments
        :param persist: should the process be persisted
        :param loader: the classloader to use
        :param nowait: if True, don't wait for the process to send a response, just return the pid
        :param no_reply: if True, this call will be fire-and-forget, i.e. no return value
        :return: the result of launching the process
        """
        ...

    def execute_process(
        self,
        process_class: str,
        init_args: Optional[Sequence[Any]] = None,
        init_kwargs: Optional[dict[str, Any]] = None,
        loader: Optional[loaders.ObjectLoader] = None,
        nowait: bool = False,
        no_reply: bool = False,
    ) -> Union[None, PID_TYPE, ProcessResult]:
        """Execute a process.  This call will first send a create task and then a continue task over
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
        ...
