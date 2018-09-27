import kiwipy
import functools
from tornado import concurrent, ioloop

from . import futures

__all__ = [
    'Action', 'Communicator', 'RemoteException', 'DeliveryFailed',
    'TaskRejected'
]

RemoteException = kiwipy.RemoteException
DeliveryFailed = kiwipy.DeliveryFailed
TaskRejected = kiwipy.TaskRejected
Communicator = kiwipy.Communicator


class Action(kiwipy.Future):
    def execute(self, publisher):
        pass


def plum_to_kiwi_future(communicator, plum_future):
    kiwi_future = communicator.create_future()
    futures.chain(plum_future, kiwi_future)
    return kiwi_future


def kiwi_to_plum_future(kiwi_future):
    plum_future = futures.Future()
    futures.chain(kiwi_future, plum_future)
    return plum_future


def convert_to_comm(communicator, loop, to_convert):
    def converted(_comm, msg):
        kiwi_future = communicator.create_future()

        def task_done(task):
            try:
                result = task.result()
                if concurrent.is_future(result):
                    result = plum_to_kiwi_future(communicator, result)
                kiwi_future.set_result(result)
            except Exception as exception:
                kiwi_future.set_exception(exception)

        msg_fn = functools.partial(to_convert, communicator, msg)
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
        converted = convert_to_comm(self._communicator, self._loop, subscriber)
        self._communicator.add_rpc_subscriber(converted, identifier)

    def remove_rpc_subscriber(self, identifier):
        self._communicator.remove_rpc_subscriber(identifier)

    def add_task_subscriber(self, subscriber):
        converted = convert_to_comm(self._communicator, self._loop, subscriber)
        self._communicator.add_task_subscriber(converted)
        self._subscribers[subscriber] = converted

    def remove_task_subscriber(self, subscriber):
        self._communicator.remove_task_subscriber(self._subscribers.pop(subscriber))

    def add_broadcast_subscriber(self, subscriber):
        converted = convert_to_comm(self._communicator, self._loop, subscriber)
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
