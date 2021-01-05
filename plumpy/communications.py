# -*- coding: utf-8 -*-
"""Module for general kiwipy communication methods"""
import asyncio
import functools
from typing import Any, Callable, Hashable, Optional, TYPE_CHECKING

import kiwipy

from . import futures
from .utils import ensure_coroutine

__all__ = [
    'Communicator', 'RemoteException', 'DeliveryFailed', 'TaskRejected', 'plum_to_kiwi_future', 'wrap_communicator'
]

RemoteException = kiwipy.RemoteException
DeliveryFailed = kiwipy.DeliveryFailed
TaskRejected = kiwipy.TaskRejected
Communicator = kiwipy.Communicator

if TYPE_CHECKING:
    # identifiers for subscribers
    ID_TYPE = Hashable  # pylint: disable=invalid-name
    Subscriber = Callable[..., Any]
    # RPC subscriber params: communicator, msg
    RpcSubscriber = Callable[[kiwipy.Communicator, Any], Any]
    # Task subscriber params: communicator, task
    TaskSubscriber = Callable[[kiwipy.Communicator, Any], Any]
    # Broadcast subscribers params: communicator, body, sender, subject, correlation id
    BroadcastSubscriber = Callable[[kiwipy.Communicator, Any, Any, Any, ID_TYPE], Any]


def plum_to_kiwi_future(plum_future: futures.Future) -> kiwipy.Future:
    """
    Return a kiwi future that resolves to the outcome of the plum future

    :param plum_future: the plum future
    :return: the kiwipy future

    """
    kiwi_future = kiwipy.Future()

    def on_done(_plum_future: futures.Future) -> None:
        with kiwipy.capture_exceptions(kiwi_future):
            if plum_future.cancelled():
                kiwi_future.cancel()
            else:
                result = plum_future.result()
                # Did we get another future?  In which case convert it too
                if isinstance(result, futures.Future):
                    result = plum_to_kiwi_future(result)
                kiwi_future.set_result(result)

    plum_future.add_done_callback(on_done)
    return kiwi_future


def convert_to_comm(callback: 'Subscriber',
                    loop: Optional[asyncio.AbstractEventLoop] = None) -> Callable[..., kiwipy.Future]:
    """
    Take a callback function and converted it to one that will schedule a callback
    on the given even loop and return a kiwi future representing the future outcome
    of the original method.

    :param loop: the even loop to schedule the callback in
    :param callback: the function to convert
    :return: a new callback function that returns a future
    """
    coro = ensure_coroutine(callback)

    def converted(communicator: kiwipy.Communicator, *args: Any, **kwargs: Any) -> kiwipy.Future:
        msg_fn = functools.partial(coro, communicator, *args, **kwargs)
        task_future = futures.create_task(msg_fn, loop)
        return plum_to_kiwi_future(task_future)

    return converted


def wrap_communicator(
    communicator: kiwipy.Communicator, loop: Optional[asyncio.AbstractEventLoop] = None
) -> 'LoopCommunicator':
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


class LoopCommunicator(kiwipy.Communicator):
    """Wrapper around a `kiwipy.Communicator` that schedules any subscriber messages on a given event loop."""

    def __init__(self, communicator: kiwipy.Communicator, loop: Optional[asyncio.AbstractEventLoop] = None):
        """
        :param communicator: The kiwipy communicator
        :param loop: The event loop to schedule callbacks on

        """
        assert communicator is not None

        self._communicator = communicator
        self._loop: asyncio.AbstractEventLoop = loop or asyncio.get_event_loop()

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
        correlation_id: Optional['ID_TYPE'] = None
    ) -> futures.Future:
        return self._communicator.broadcast_send(body, sender, subject, correlation_id)

    def is_closed(self) -> bool:
        """Return `True` if the communicator was closed"""
        return self._communicator.is_closed()

    def close(self) -> None:
        """Close a communicator, free up all resources and do not allow any further operations"""
        self._communicator.close()
