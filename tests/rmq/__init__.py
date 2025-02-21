# -*- coding: utf-8 -*-
from __future__ import annotations
from re import Pattern
from typing import TYPE_CHECKING, Generic, Hashable, TypeVar, final
import kiwipy
import concurrent.futures

from plumpy.exceptions import CoordinatorConnectionError

if TYPE_CHECKING:
    ID_TYPE = Hashable
    BroadcastSubscriber = Callable[[Any, Any, Any, ID_TYPE], Any]

U = TypeVar('U', bound=kiwipy.Communicator)


@final
class RmqCoordinator(Generic[U]):
    def __init__(self, comm: U):
        self._comm = comm

    @property
    def communicator(self) -> U:
        """The inner communicator."""
        return self._comm

    # XXX: naming - `add_receiver_rpc`
    def add_rpc_subscriber(self, subscriber, identifier=None):
        def _subscriber(_, *args, **kwargs):
            return subscriber(*args, **kwargs)

        return self._comm.add_rpc_subscriber(_subscriber, identifier)

    # XXX: naming - `add_receiver_broadcast`
    def add_broadcast_subscriber(
        self,
        subscriber: 'BroadcastSubscriber',
        subject_filters: list[Hashable | Pattern[str]] | None = None,
        sender_filters: list[Hashable | Pattern[str]] | None = None,
        identifier: 'ID_TYPE | None' = None,
    ):
        def _subscriber(_, *args, **kwargs):
            return subscriber(*args, **kwargs)

        return self._comm.add_broadcast_subscriber(_subscriber, identifier)

    # XXX: naming - `add_reciver_task` (can be combined with two above maybe??)
    def add_task_subscriber(self, subscriber, identifier=None):
        async def _subscriber(_comm, *args, **kwargs):
            return await subscriber(*args, **kwargs)

        return self._comm.add_task_subscriber(_subscriber, identifier)

    def remove_rpc_subscriber(self, identifier):
        return self._comm.remove_rpc_subscriber(identifier)

    def remove_broadcast_subscriber(self, identifier):
        return self._comm.remove_broadcast_subscriber(identifier)

    def remove_task_subscriber(self, identifier):
        return self._comm.remove_task_subscriber(identifier)

    # XXX: naming - `send_to`
    def rpc_send(self, recipient_id, msg):
        return self._comm.rpc_send(recipient_id, msg)

    # XXX: naming - `broadcast`
    def broadcast_send(
        self,
        body,
        sender=None,
        subject=None,
        correlation_id=None,
    ):
        from aio_pika.exceptions import ChannelInvalidStateError, AMQPConnectionError

        try:
            rsp = self._comm.broadcast_send(body, sender, subject, correlation_id)
        except (ChannelInvalidStateError, AMQPConnectionError, concurrent.futures.TimeoutError) as exc:
            raise CoordinatorConnectionError from exc
        else:
            return rsp

    # XXX: naming - `assign_task` (this may able to be combined with send_to)
    def task_send(self, task, no_reply=False):
        return self._comm.task_send(task, no_reply)

    def close(self):
        self._comm.close()
