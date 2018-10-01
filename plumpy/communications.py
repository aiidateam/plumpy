"""Module for general kiwipy communication methods"""

from __future__ import absolute_import
import kiwipy
import functools
from tornado import concurrent, ioloop

from . import futures

__all__ = [
    'Communicator', 'RemoteException', 'DeliveryFailed', 'TaskRejected', 'kiwi_to_plum_future', 'plum_to_kiwi_future'
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
    futures.chain(plum_future, kiwi_future)
    return kiwi_future


def kiwi_to_plum_future(kiwi_future, loop=None):
    """
    Return a plum future that resolves to the outcome of the kiwi future

    :param kiwi_future: the kiwi future
    :type kiwi_future: :class:`kiwipy.Future`
    :param loop: the event loop to schedule the callback on
    :type loop: :class:`tornado.ioloop.IOLoop`
    :return: the tornado future
    :rtype: :class:`plumpy.Future`
    """
    loop = loop or ioloop.IOLoop.current()

    tornado_future = futures.Future()

    def done(done_future):
        if done_future.cancelled():
            tornado_future.cancel()

        with kiwipy.capture_exceptions(tornado_future):
            result = done_future.result()
            if isinstance(result, kiwipy.Future):
                result = kiwi_to_plum_future(result, loop)

            tornado_future.set_result(result)

    loop.add_future(kiwi_future, done)
    return tornado_future


def convert_to_comm(loop, callback):
    """
    Take a callback function and converted it to one that will schedule a callback
    on the given even loop and return a kiwi future representing the future outcome
    of the original method.

    :param loop: the even loop to schedule the callback in
    :param callback: the function to convert
    :return: a new callback function that returns a future
    """

    def converted(communicator, msg):
        kiwi_future = kiwipy.Future()

        def task_done(task):
            with kiwipy.capture_exceptions(kiwi_future):
                result = task.result()
                if concurrent.is_future(result):
                    result = plum_to_kiwi_future(result)
                kiwi_future.set_result(result)

        msg_fn = functools.partial(callback, communicator, msg)
        task_future = futures.create_task(msg_fn, loop)
        task_future.add_done_callback(task_done)

        return kiwi_future

    return converted


class CommunicatorWrapper(kiwipy.Communicator):
    """
    This wrapper takes a kiwipy Communicator and schedules and messages on a given
    event loop passing back an appropriate kiwipy future that will be resolved with
    the result of the callback.
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

    def add_rpc_subscriber(self, subscriber, identifier):
        converted = convert_to_comm(self._loop, subscriber)
        self._communicator.add_rpc_subscriber(converted, identifier)

    def remove_rpc_subscriber(self, identifier):
        self._communicator.remove_rpc_subscriber(identifier)

    def add_task_subscriber(self, subscriber):
        converted = convert_to_comm(self._loop, subscriber)
        self._communicator.add_task_subscriber(converted)
        self._subscribers[subscriber] = converted

    def remove_task_subscriber(self, subscriber):
        self._communicator.remove_task_subscriber(self._subscribers.pop(subscriber))

    def add_broadcast_subscriber(self, subscriber):
        converted = convert_to_comm(self._loop, subscriber)
        self._communicator.add_broadcast_subscriber(converted)
        self._subscribers[subscriber] = converted

    def remove_broadcast_subscriber(self, subscriber):
        self._communicator.remove_task_subscriber(self._subscribers.pop(subscriber))

    def task_send(self, msg):
        return self._communicator.task_send(msg)

    def rpc_send(self, recipient_id, msg):
        return self._communicator.rpc_send(recipient_id, msg)

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        return self._communicator.broadcast_send(body, sender, subject, correlation_id)

    def wait_for(self, future, timeout=None):
        return self._communicator.wait_for(future, timeout)
