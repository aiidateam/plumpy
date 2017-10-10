import abc
import apricotpy
from apricotpy import persistable
from collections import namedtuple
import json
import logging
import pika
import pickle
import traceback
import uuid

from plum import process
from plum.rmq.defaults import Defaults
from plum.utils import override, load_class, fullname

_LOGGER = logging.getLogger(__name__)

__all__ = ['ProcessLaunchSubscriber', 'ProcessLaunchPublisher']

_RunningTaskInfo = namedtuple("_RunningTaskInfo", ['pid', 'ch', 'delivery_tag'])


class ProcessLaunchSubscriber(apricotpy.TickingLoopObject):
    """
    Run tasks as they come form the RabbitMQ task queue

    .. warning:: the pika library used is not thread safe and as such the
        connection passed to the constructor must be created on the same thread
        as the start method is called.
    """

    def __init__(self, connection, queue=Defaults.TASK_QUEUE, decoder=json.loads,
                 response_encoder=json.dumps, persistent_uuid=None):
        """
        :param connection: The pika RabbitMQ connection
        :type connection: :class:`pika.Connection`
        :param queue: The queue name to use
        :param decoder: A function to deserialise incoming messages
        """
        super(ProcessLaunchSubscriber, self).__init__()

        if persistent_uuid is not None:
            self._uuid = uuid

        self._decode = decoder
        self._response_encode = response_encoder
        self._stopping = False
        self._num_processes = 0

        # Set up communications
        self._channel = connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._channel.queue_declare(queue=queue, durable=True)
        self._channel.basic_consume(self._on_launch, queue=queue)

    @override
    def tick(self):
        """
        Poll the channel for launch process events
        """
        self._channel.connection.process_data_events()

    def _on_launch(self, ch, method, props, body):
        """
        Consumer function that processes the launch message.

        :param ch: The channel
        :param method: The method
        :param props: The message properties
        :param body: The message body
        """
        self._num_processes += 1

        try:
            task = self._decode(body)
        except BaseException:
            response = {
                'state': 'exception',
                'exception': 'Failed to decode task: {}'.format(
                    traceback.format_exception(type(exc), exc, None)[0]
                )
            }
            self._channel.basic_publish(
                exchange='', routing_key=props.reply_to,
                properties=pika.BasicProperties(
                    correlation_id=props.correlation_id
                ),
                body=self._response_encode(response)
            )

        try:
            proc = self.loop().create(task.proc_class, *task.args, **task.kwargs)
        except BaseException as exc:
            response = {
                'pid': task.kwargs.get('pid', None),
                'state': 'exception',
                'exception': 'Failed to create the process: {}'.format(
                    traceback.format_exception(type(exc), exc, None)[0]
                )
            }
            self._channel.basic_publish(
                exchange='', routing_key=props.reply_to,
                properties=pika.BasicProperties(
                    correlation_id=props.correlation_id
                ),
                body=self._response_encode(response)
            )
            self._channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            # Tell the sender that we've launched it
            ch.basic_publish(
                exchange='', routing_key=props.reply_to,
                properties=pika.BasicProperties(
                    correlation_id=props.correlation_id,
                    delivery_mode=2
                ),
                body=self._response_encode({'pid': proc.pid, 'state': 'launched'})
            )

            proc.add_done_callback(persistable.Function(
                _process_done,
                persistable.ObjectProxy(self),
                method.delivery_tag,
                props.reply_to,
                props.correlation_id
            ))


def _process_done(publisher, delivery_tag, reply_to, correlation_id, process):
    """
    :param publisher: The process launch subscriber
    :type publisher: :class:`ProcessLaunchSubscriber`
    :param delivery_tag:
    :param reply_to:
    :param correlation_id:
    :param process:
    """
    response = {'pid': process.pid}

    if process.cancelled():
        response['state'] = 'cancelled'
    elif process.exception() is not None:
        response['state'] = 'exception'
        exc = process.exception()
        response['exception'] = traceback.format_exception(type(exc), exc, None)[0]
    else:
        response['state'] = 'finished'
        response['result'] = process.result()

    # Tell the sender that we've finished
    publisher._channel.basic_publish(
        exchange='', routing_key=reply_to,
        properties=pika.BasicProperties(correlation_id=correlation_id),
        body=publisher._response_encode(response)
    )
    publisher._channel.basic_ack(delivery_tag=delivery_tag)


LaunchResponse = namedtuple('LaunchResponse', ['pid', 'done_future'])


class ProcessLaunchPublisher(apricotpy.TickingLoopObject):
    """
    Class used to publishes messages requesting a process to be launched
    """

    def __init__(self, connection, queue=Defaults.TASK_QUEUE, encoder=pickle.dumps,
                 response_decoder=json.loads):
        super(ProcessLaunchPublisher, self).__init__()

        self._queue = queue
        self._encode = encoder
        self._response_decode = response_decoder
        self._responses = {}
        self._channel = connection.channel()
        self._channel.queue_declare(queue=queue, durable=True)

        # Response queue
        result = self._channel.queue_declare(exclusive=True, durable=True)
        self._callback_queue = result.method.queue
        self._channel.basic_consume(self._on_response, no_ack=False, queue=self._callback_queue)

    def launch(self, process_bundle):
        """
        Send a request to continue a Process from the provided bundle

        :param process_bundle: The Process bundle to run
        """
        self._assert_in_loop()

        await_done = self.loop().create(_AwaitDone, self, process_bundle)

        if not await_done.done():
            self._responses[await_done._correlation_id] = await_done

        return await_done

    @override
    def tick(self):
        self._channel.connection.process_data_events()

    def _on_response(self, ch, method, props, body):
        corr_id = props.correlation_id

        await_done = self._responses.get(corr_id, None)

        if await_done is not None:
            await_done.on_response(ch, method, props, body)
            if await_done.done():
                del self._responses[corr_id]

    def _assert_in_loop(self):
        assert self.in_loop(), "Object is not in the event loop"


class _AwaitDone(persistable.AwaitableLoopObject):
    PUBLISHER = 'PUBLISHER'
    CORRELATION_ID = 'CORRELATION_ID'
    CONSUMER_TAG = 'CONSUMER_TAG'

    __metaclass__ = abc.ABCMeta

    def __init__(self, publisher, process_bundle):
        super(_AwaitDone, self).__init__()

        self._pid = process.get_pid_from_bundle(process_bundle)
        self._publisher = publisher
        self._consumer_tag = None
        self._correlation_id = str(uuid.uuid4())

        delivered = publisher._channel.basic_publish(
            exchange='', routing_key=publisher._queue,
            body=self._publisher._encode(process_bundle),
            properties=pika.BasicProperties(
                reply_to=publisher._callback_queue,
                delivery_mode=2,
                correlation_id=self._correlation_id
            )
        )

        if not delivered:
            raise RuntimeError("Failed to launch task")

    @property
    def pid(self):
        return self._pid

    def save_instance_state(self, out_state):
        super(_AwaitDone, self).save_instance_state(out_state)

        out_state[self.PUBLISHER] = persistable.ObjectProxy(self._publisher)
        out_state[self.CORRELATION_ID] = self._correlation_id
        out_state[self.CONSUMER_TAG] = self._consumer_tag

    def load_instance_state(self, saved_state):
        super(_AwaitDone, self).load_instance_state(saved_state)

        self._publisher = saved_state[self.PUBLISHER]
        self._correlation_id = saved_state[self.CORRELATION_ID]
        self._consumer_tag = saved_state[self.CONSUMER_TAG]

        # TODO: Ask the connection to resend messages

    def on_response(self, ch, method, props, body):
        assert props.correlation_id == self._correlation_id

        response = self._publisher._response_decode(body)

        if response['state'] == 'cancelled':
            self.cancel()
        elif response['state'] == 'exception':
            self.set_exception(RuntimeError(response['exception']))
        elif response['state'] == 'finished':
            self.set_result(response['result'])
