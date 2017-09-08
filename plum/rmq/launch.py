import apricotpy
from apricotpy import persistable
from collections import namedtuple
import json
import pika
import traceback
import uuid

from plum.rmq.defaults import Defaults
from plum.utils import override, load_class, fullname

__all__ = ['ProcessLaunchSubscriber', 'ProcessLaunchPublisher']

_RunningTaskInfo = namedtuple("_RunningTaskInfo", ['pid', 'ch', 'delivery_tag'])


def launched_decode(msg):
    d = json.loads(msg)
    if isinstance(d['pid'], basestring):
        try:
            d['pid'] = uuid.UUID(d['pid'])
        except ValueError:
            pass
    return d


def launched_encode(msg):
    d = msg.copy()
    if isinstance(d['pid'], uuid.UUID):
        d['pid'] = str(d['pid'])
    return json.dumps(d)


class ProcessLaunchSubscriber(apricotpy.TickingLoopObject):
    """
    Run tasks as they come form the RabbitMQ task queue

    .. warning:: the pika library used is not thread safe and as such the
        connection passed to the constructor must be created on the same thread
        as the start method is called.
    """

    def __init__(self, connection, queue=Defaults.TASK_QUEUE, decoder=json.loads,
                 response_encoder=launched_encode, persistent_uuid=None):
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

        task = self._decode(body)
        proc_class = load_class(task['proc_class'])

        proc = self.loop().create(proc_class, inputs=task['inputs'])

        # Tell the sender that we've launched it
        ch.basic_publish(
            exchange='', routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
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
        response['result'] = str(process.result())

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

    def __init__(self, connection, queue=Defaults.TASK_QUEUE, encoder=json.dumps,
                 response_decoder=launched_decode):
        super(ProcessLaunchPublisher, self).__init__()

        self._queue = queue
        self._encode = encoder
        self._response_decode = response_decoder
        self._responses = {}

        self._channel = connection.channel()
        self._channel.queue_declare(queue=queue, durable=True)
        # Response queue
        result = self._channel.queue_declare(exclusive=True)
        self._callback_queue = result.method.queue
        self._channel.basic_consume(self._on_response, no_ack=True, queue=self._callback_queue)

    def launch(self, proc_class, inputs=None):
        """
        Send a request to launch a Process.

        :param proc_class: The Process class to run
        :param inputs: The inputs to supply
        """
        self._assert_in_loop()

        future = self.loop().create_future()

        correlation_id = str(uuid.uuid4())
        task = {'proc_class': fullname(proc_class), 'inputs': inputs}
        delivered = self._channel.basic_publish(
            exchange='', routing_key=self._queue, body=self._encode(task),
            properties=pika.BasicProperties(
                reply_to=self._callback_queue,
                delivery_mode=2,  # Persist
                correlation_id=correlation_id
            )
        )

        if delivered:
            self._responses[correlation_id] = future
        else:
            return future.set_exception(RuntimeError("Failed to delivery message to exchange"))

        return future

    @override
    def tick(self):
        self._channel.connection.process_data_events()

    def _on_response(self, ch, method, props, body):
        if props.correlation_id in self._responses:
            response = self._response_decode(body)
            if response['state'] == 'launched':
                future = self._responses[props.correlation_id]

                done_future = self.loop().create_future()
                result = LaunchResponse(response['pid'], done_future)
                self._responses[props.correlation_id] = done_future

                future.set_result(result)
            else:
                future = self._responses.pop(props.correlation_id)
                future.set_result(response)

    def _assert_in_loop(self):
        assert self.in_loop(), "Object is not in the event loop"
