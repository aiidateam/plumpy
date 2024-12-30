# -*- coding: utf-8 -*-
"""Module for general kiwipy communication methods"""

from __future__ import annotations

import asyncio
import functools
from typing import TYPE_CHECKING, Any, Callable, Generic, Hashable, Optional, TypeVar, final

import kiwipy

from plumpy.futures import create_task
from plumpy.rmq.futures import wrap_to_concurrent_future
from plumpy.utils import ensure_coroutine

__all__ = [
    'Communicator',
    'DeliveryFailed',
    'RemoteException',
    'TaskRejected',
    'wrap_communicator',
]

RemoteException = kiwipy.RemoteException
DeliveryFailed = kiwipy.DeliveryFailed
TaskRejected = kiwipy.TaskRejected
Communicator = kiwipy.Communicator

if TYPE_CHECKING:
    # identifiers for subscribers
    ID_TYPE = Hashable
    Subscriber = Callable[..., Any]
    # RPC subscriber params: communicator, msg
    RpcSubscriber = Callable[[kiwipy.Communicator, Any], Any]
    # Task subscriber params: communicator, task
    TaskSubscriber = Callable[[kiwipy.Communicator, Any], Any]
    # Broadcast subscribers params: communicator, body, sender, subject, correlation id
    BroadcastSubscriber = Callable[[kiwipy.Communicator, Any, Any, Any, ID_TYPE], Any]


def convert_to_comm(
    callback: 'Subscriber', loop: Optional[asyncio.AbstractEventLoop] = None
) -> Callable[..., kiwipy.Future]:
    """
    Take a callback function and converted it to one that will schedule a callback
    on the given even loop and return a kiwi future representing the future outcome
    of the original method.

    :param callback: the function to convert
    :param loop: the even loop to schedule the callback in
    :return: a new callback function that returns a future
    """
    if isinstance(callback, kiwipy.BroadcastFilter):
        # if the broadcast is filtered for this callback,
        # we don't want to go through the (costly) process
        # of setting up async tasks and callbacks

        def _passthrough(*args: Any, **kwargs: Any) -> bool:
            sender = kwargs.get('sender', args[1])
            subject = kwargs.get('subject', args[2])
            return callback.is_filtered(sender, subject)
    else:

        def _passthrough(*args: Any, **kwargs: Any) -> bool:
            return False

    coro = ensure_coroutine(callback)

    def converted(communicator: kiwipy.Communicator, *args: Any, **kwargs: Any) -> kiwipy.Future:
        if _passthrough(*args, **kwargs):
            kiwi_future = kiwipy.Future()
            kiwi_future.set_result(None)
            return kiwi_future

        msg_fn = functools.partial(coro, communicator, *args, **kwargs)
        task_future = create_task(msg_fn, loop)
        return wrap_to_concurrent_future(task_future)

    return converted

T = TypeVar('T', bound=kiwipy.Communicator)

def wrap_communicator(
    communicator: T, loop: Optional[asyncio.AbstractEventLoop] = None
) -> 'LoopCommunicator[T]':
    """
    Wrap a communicator such that all callbacks made to any subscribers are scheduled on the
    given event loop.

    If the communicator is already an equivalent communicator wrapper then it will not be
    wrapped again.

    :param communicator: the communicator to wrap
    :param loop: the event loop to schedule callbacks on

    :return: a communicator wrapper

    """
    if isinstance(communicator, LoopCommunicator) and communicator.loop() is loop:
        return communicator

    return LoopCommunicator(communicator, loop)


@final
class LoopCommunicator(Generic[T], kiwipy.Communicator):  # type: ignore
    """Wrapper around a `kiwipy.Communicator` that schedules any subscriber messages on a given event loop."""

    def __init__(self, communicator: T, loop: Optional[asyncio.AbstractEventLoop] = None):
        """
        :param communicator: The kiwipy communicator
        :param loop: The event loop to schedule callbacks on

        """
        assert communicator is not None

        self._communicator = communicator
        self._loop: asyncio.AbstractEventLoop = loop or asyncio.get_event_loop()

    @property
    def inner(self) -> T:
        return self._communicator

    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def add_rpc_subscriber(self, subscriber: 'RpcSubscriber', identifier: Optional['ID_TYPE'] = None) -> 'ID_TYPE':
        converted = convert_to_comm(subscriber, self._loop)
        return self._communicator.add_rpc_subscriber(converted, identifier)

    def remove_rpc_subscriber(self, identifier: 'ID_TYPE') -> None:
        return self._communicator.remove_rpc_subscriber(identifier)

    def add_task_subscriber(self, subscriber: 'TaskSubscriber', identifier: Optional['ID_TYPE'] = None) -> 'ID_TYPE':
        converted = convert_to_comm(subscriber, self._loop)
        return self._communicator.add_task_subscriber(converted, identifier)

    def remove_task_subscriber(self, identifier: 'ID_TYPE') -> None:
        return self._communicator.remove_task_subscriber(identifier)

    def add_broadcast_subscriber(
        self, subscriber: 'BroadcastSubscriber', identifier: Optional['ID_TYPE'] = None
    ) -> 'ID_TYPE':
        converted = convert_to_comm(subscriber, self._loop)
        return self._communicator.add_broadcast_subscriber(converted, identifier)

    def remove_broadcast_subscriber(self, identifier: 'ID_TYPE') -> None:
        return self._communicator.remove_broadcast_subscriber(identifier)

    def task_send(self, task: Any, no_reply: bool = False) -> kiwipy.Future:
        return self._communicator.task_send(task, no_reply)

    def rpc_send(self, recipient_id: 'ID_TYPE', msg: Any) -> kiwipy.Future:
        return self._communicator.rpc_send(recipient_id, msg)

    def broadcast_send(
        self,
        body: Optional[Any],
        sender: Optional[str] = None,
        subject: Optional[str] = None,
        correlation_id: Optional['ID_TYPE'] = None,
    ) -> kiwipy.Future:
        return self._communicator.broadcast_send(body, sender, subject, correlation_id)

    def is_closed(self) -> bool:
        """Return `True` if the communicator was closed"""
        return self._communicator.is_closed()

    def close(self) -> None:
        """Close a communicator, free up all resources and do not allow any further operations"""
        self._communicator.close()
