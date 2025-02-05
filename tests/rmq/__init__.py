# -*- coding: utf-8 -*-
import kiwipy
import concurrent.futures

from plumpy.exceptions import CoordinatorConnectionError


class RmqCoordinator:
    def __init__(self, comm: kiwipy.Communicator):
        self._comm = comm

    # XXX: naming - `add_receiver_rpc`
    def add_rpc_subscriber(self, subscriber, identifier=None):
        return self._comm.add_rpc_subscriber(subscriber, identifier)

    # XXX: naming - `add_receiver_broadcast`
    def add_broadcast_subscriber(
        self,
        subscriber,
        subject_filters=None,
        identifier=None,
    ):
        subscriber = kiwipy.BroadcastFilter(subscriber, subject=subject_filters)
        return self._comm.add_broadcast_subscriber(subscriber, identifier)

    # XXX: naming - `add_reciver_task` (can be combined with two above maybe??)
    def add_task_subscriber(self, subscriber, identifier=None):
        return self._comm.add_task_subscriber(subscriber, identifier)

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
