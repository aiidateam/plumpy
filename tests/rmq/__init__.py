# -*- coding: utf-8 -*-
from __future__ import annotations
from re import Pattern
from typing import TYPE_CHECKING, Any, Callable, Generic, Hashable, TypeVar, final
import kiwipy
import concurrent.futures

from plumpy.exceptions import CoordinatorConnectionError

if TYPE_CHECKING:
    ID_TYPE = Hashable
    Receiver = Callable[..., Any]

U = TypeVar('U', bound=kiwipy.Communicator)


@final
class RmqCoordinator(Generic[U]):
    def __init__(self, comm: U):
        self._comm = comm

    @property
    def communicator(self) -> U:
        """The inner communicator."""
        return self._comm

    def hook_rpc_receiver(
        self,
        receiver: 'Receiver',
        identifier: 'ID_TYPE | None' = None,
    ) -> Any:
        def _subscriber(_, *args, **kwargs):
            return receiver(*args, **kwargs)

        return self._comm.add_rpc_subscriber(_subscriber, identifier)

    def hook_broadcast_receiver(
        self,
        receiver: 'Receiver',
        subject_filters: list[Hashable | Pattern[str]] | None = None,
        sender_filters: list[Hashable | Pattern[str]] | None = None,
        identifier: 'ID_TYPE | None' = None,
    ) -> Any:
        def _subscriber(_, *args, **kwargs):
            return receiver(*args, **kwargs)

        return self._comm.add_broadcast_subscriber(_subscriber, identifier)

    def hook_task_receiver(
        self,
        receiver: 'Receiver',
        identifier: 'ID_TYPE | None' = None,
    ) -> 'ID_TYPE':
        async def _subscriber(_comm, *args, **kwargs):
            return await receiver(*args, **kwargs)

        return self._comm.add_task_subscriber(_subscriber, identifier)

    def unhook_rpc_receiver(self, identifier: 'ID_TYPE | None') -> None:
        return self._comm.remove_rpc_subscriber(identifier)

    def unhook_broadcast_receiver(self, identifier: 'ID_TYPE | None') -> None:
        return self._comm.remove_broadcast_subscriber(identifier)

    def unhook_task_receiver(self, identifier: 'ID_TYPE') -> None:
        return self._comm.remove_task_subscriber(identifier)

    def rpc_send(
        self,
        recipient_id: Hashable,
        msg: Any,
    ) -> Any:
        return self._comm.rpc_send(recipient_id, msg)

    def broadcast_send(
        self,
        body: Any | None,
        sender: 'ID_TYPE | None' = None,
        subject: str | None = None,
        correlation_id: 'ID_TYPE | None' = None,
    ) -> Any:
        from aio_pika.exceptions import ChannelInvalidStateError, AMQPConnectionError

        try:
            rsp = self._comm.broadcast_send(body, sender, subject, correlation_id)
        except (ChannelInvalidStateError, AMQPConnectionError, concurrent.futures.TimeoutError) as exc:
            raise CoordinatorConnectionError from exc
        else:
            return rsp

    def task_send(self, task: Any, no_reply: bool = False) -> Any:
        return self._comm.task_send(task, no_reply)

    def close(self) -> None:
        self._comm.close()
