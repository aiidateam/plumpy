# -*- coding: utf-8 -*-
from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any, Dict, Optional

from typing_extensions import Self

from plumpy.loaders import ObjectLoader
from plumpy.persistence import LoadSaveContext, auto_save, ensure_object_loader

from . import persistence
from .utils import SAVED_STATE_TYPE

if TYPE_CHECKING:
    from .processes import Process


@persistence.auto_persist('_params')
class ProcessListener(metaclass=abc.ABCMeta):
    # region Persistence methods

    def __init__(self) -> None:
        super().__init__()
        self._params: Dict[str, Any] = {}

    def init(self, **kwargs: Any) -> None:
        self._params = kwargs

    @classmethod
    def recreate_from(cls, saved_state: SAVED_STATE_TYPE, load_context: Optional[LoadSaveContext] = None) -> Self:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = cls.__new__(cls)
        obj.init(**saved_state['_params'])
        return obj

    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = auto_save(self, loader)

        return out_state

    # endregion

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
