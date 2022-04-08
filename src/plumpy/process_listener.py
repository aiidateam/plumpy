# -*- coding: utf-8 -*-
import abc
from typing import TYPE_CHECKING, Any

__all__ = ['ProcessListener']

if TYPE_CHECKING:
    from .processes import Process  # pylint: disable=cyclic-import


class ProcessListener(metaclass=abc.ABCMeta):

    def on_process_created(self, process: 'Process') -> None:
        """
        Called when the process has been started

        :param process: The process

        """

    def on_process_running(self, process: 'Process') -> None:
        """
        Called when the process is about to enter the RUNNING state

        :param process: The process

        """

    def on_process_waiting(self, process: 'Process') -> None:
        """
        Called when the process is about to enter the WAITING state

        :param process: The process

        """

    def on_process_paused(self, process: 'Process') -> None:
        """
        Called when the process is about to re-enter the RUNNING state

        :param process: The process

        """

    def on_process_played(self, process: 'Process') -> None:
        """
        Called when the process is about to re-enter the RUNNING state

        :param process: The process

        """

    def on_output_emitted(self, process: 'Process', output_port: str, value: Any, dynamic: bool) -> None:
        """
        Called when the process has emitted an output value

        :param process: The process
        :param output_port: The output port that the value was outputted on
        :param value: The value that was outputted
        :param dynamic: True if the port is dynamic, False otherwise

        """

    def on_process_finished(self, process: 'Process', outputs: Any) -> None:
        """
        Called when the process has finished successfully

        :param process: The process
        :param outputs: The process outputs

        """

    def on_process_excepted(self, process: 'Process', reason: str) -> None:
        """
        Called when the process has excepted

        :param process: The process
        :param reason: A string of the exception message

        """

    def on_process_killed(self, process: 'Process', msg: str) -> None:
        """
        Called when the process was killed

        :param process: The process

        """
