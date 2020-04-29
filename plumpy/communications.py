# -*- coding: utf-8 -*-
"""Module for general kiwipy communication methods"""

import functools

from tornado import concurrent, ioloop

import kiwipy

from . import futures

__all__ = [
    'Communicator', 'RemoteException', 'DeliveryFailed', 'TaskRejected', 'plum_to_kiwi_future', 'wrap_communicator'
]

RemoteException = kiwipy.RemoteException
DeliveryFailed = kiwipy.DeliveryFailed
TaskRejected = kiwipy.TaskRejected
Communicator = kiwipy.Communicator


def plum_to_kiwi_future(plum_future):
    """
    Return a kiwi future that resolves to the outcome of the plum future

    :param plum_future: the plum future
    :type plum_future: :class:`plumpy.Future`
    :return: the kiwipy future
    :rtype: :class:`kiwipy.Future`
    """
    kiwi_future = kiwipy.Future()

    def on_done(_plum_future):
        with kiwipy.capture_exceptions(kiwi_future):
            if plum_future.cancelled():
                kiwi_future.cancel()
            else:
                result = plum_future.result()
                # Did we get another future?  In which case convert it too
                if concurrent.is_future(result):
                    result = plum_to_kiwi_future(result)
                kiwi_future.set_result(result)

    plum_future.add_done_callback(on_done)
    return kiwi_future


def convert_to_comm(callback, loop=None):
    """
    Take a callback function and converted it to one that will schedule a callback
    on the given even loop and return a kiwi future representing the future outcome
    of the original method.

    :param loop: the even loop to schedule the callback in
    :param callback: the function to convert
    :return: a new callback function that returns a future
    """
    loop = loop or ioloop.IOLoop.current()

    def converted(communicator, *args, **kwargs):
        msg_fn = functools.partial(callback, communicator, *args, **kwargs)
        task_future = futures.create_task(msg_fn, loop)
        return plum_to_kiwi_future(task_future)

    return converted


def wrap_communicator(communicator, loop=None):
    """
    Wrap a communicator such that all callbacks made to any subscribers are scheduled on the
    given event loop.

    If the communicator is already an equivalent communicator wrapper then it will not be
    wrapped again.

    :param communicator: the communicator to wrap
    :type communicator: :class:`kiwipy.Communicator`
    :param loop: the event loop to schedule callbacks on
    :type loop: :class:`tornado.ioloop.IOLoop`
    :return: a communicator wrapper
    :rtype: :class:`plumpy.LoopCommunicator`
    """
    if isinstance(communicator, LoopCommunicator) and communicator.loop() is loop:
        return communicator

    return LoopCommunicator(communicator, loop)


class LoopCommunicator(kiwipy.Communicator):
    """
    This wrapper takes a kiwipy Communicator and schedules any subscriber messages on a given
    event loop.
    """

    def __init__(self, communicator, loop=None):
        """
        :param communicator: The kiwipy communicator
        :type communicator: :class:`kiwipy.Communicator`
        :param loop: The tornado event loop to schedule callbacks on
        :type loop: :class:`tornado.ioloop.IOLoop`
        """
        assert communicator is not None

        self._communicator = communicator
        self._loop = loop or ioloop.IOLoop.current()
        self._subscribers = {}

    def loop(self):
        return self._loop

    def add_rpc_subscriber(self, subscriber, identifier=None):
        converted = convert_to_comm(subscriber, self._loop)
        return self._communicator.add_rpc_subscriber(converted, identifier)

    def remove_rpc_subscriber(self, identifier):
        self._communicator.remove_rpc_subscriber(identifier)

    def add_task_subscriber(self, subscriber):
        converted = convert_to_comm(subscriber, self._loop)
        self._communicator.add_task_subscriber(converted)
        self._subscribers[subscriber] = converted

    def remove_task_subscriber(self, subscriber):
        self._communicator.remove_task_subscriber(self._subscribers.pop(subscriber))

    def add_broadcast_subscriber(self, subscriber, identifier=None):
        converted = convert_to_comm(subscriber, self._loop)
        identifier = self._communicator.add_broadcast_subscriber(converted, identifier)
        self._subscribers[identifier] = converted
        return identifier

    def remove_broadcast_subscriber(self, identifier):
        self._communicator.remove_task_subscriber(self._subscribers.pop(identifier))

    def task_send(self, task, no_reply=False):
        return self._communicator.task_send(task, no_reply)

    def rpc_send(self, recipient_id, msg):
        return self._communicator.rpc_send(recipient_id, msg)

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        return self._communicator.broadcast_send(body, sender, subject, correlation_id)
