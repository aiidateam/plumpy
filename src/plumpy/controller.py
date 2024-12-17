from collections.abc import Sequence
from typing import Any, Protocol

from plumpy import loaders
from plumpy.message import MessageType
from plumpy.utils import PID_TYPE

ProcessResult = Any
ProcessStatus = Any


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

    def pause_process(self, pid: 'PID_TYPE', msg: Any | None = None) -> ProcessResult:
        """
        Pause the process

        :param pid: the pid of the process to pause
        :param msg: optional pause message
        :return: True if paused, False otherwise
        """
        ...

    def play_process(self, pid: 'PID_TYPE') -> ProcessResult:
        """
        Play the process

        :param pid: the pid of the process to play
        :return: True if played, False otherwise
        """
        ...

    def kill_process(self, pid: 'PID_TYPE', msg: MessageType | None = None) -> ProcessResult:
        """
        Kill the process

        :param pid: the pid of the process to kill
        :param msg: optional kill message
        :return: True if killed, False otherwise
        """
        ...

    def continue_process(
        self, pid: 'PID_TYPE', tag: str|None = None, nowait: bool = False, no_reply: bool = False
    ) -> ProcessResult | None:
        """
        Continue the process

        :param _communicator: the communicator
        :param pid: the pid of the process to continue
        :param tag: the checkpoint tag to continue from
        """
        ...

    async def launch_process(
        self,
        process_class: str,
        init_args: Sequence[Any] | None = None,
        init_kwargs: dict[str, Any] | None = None,
        persist: bool = False,
        loader: loaders.ObjectLoader | None = None,
        nowait: bool = False,
        no_reply: bool = False,
    ) -> ProcessResult:
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
        ...

    async def execute_process(
        self,
        process_class: str,
        init_args: Sequence[Any] | None = None,
        init_kwargs: dict[str, Any] | None = None,
        loader: loaders.ObjectLoader | None = None,
        nowait: bool = False,
        no_reply: bool = False,
    ) -> ProcessResult:
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
        ...
