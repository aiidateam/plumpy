import abc
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
        #self._channel.basic_consume(self._on_response, no_ack=False, queue=self._callback_queue)

    def launch2(self, proc_class, inputs=None):
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

    def launch(self, proc_class, inputs=None):
        """
        Send a request to launch a Process.

        :param proc_class: The Process class to run
        :param inputs: The inputs to supply
        """
        self._assert_in_loop()

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

        if not delivered:
            raise RuntimeError("Failed to delivery message to exchange")

        return self.loop().create(_AwaitLaunch, self, correlation_id)

    @override
    def tick(self):
        self._channel.connection.process_data_events()

    def _create_launch_message(self, process_class, inputs):
        """
        Create a string that encodes the message interpreted by the launch subscriber

        :param process_class: The process class to launch at the remote end
        :param inputs: The inputs
        :type inputs: dict
        :return: The launch task encoded in a string
        :rtype: str
        """
        task = {'proc_class': fullname(process_class), 'inputs': inputs}
        return self._encode(task)

    def _on_response(self, ch, method, props, body):
        if props.correlation_id in self._responses:
            response = self._response_decode(body)
            future = self._responses.pop(props.correlation_id)

            if response['state'] == 'launched':
                done_future = self.loop().create_future()
                result = LaunchResponse(response['pid'], done_future)
                self._responses[props.correlation_id] = done_future

                future.set_result(result)
            else:
                future.set_result(response)

    def _consume_responses(self, callback):
        return self._channel.basic_consume(
            callback, no_ack=False, queue=self._callback_queue)

    def _assert_in_loop(self):
        assert self.in_loop(), "Object is not in the event loop"


class _AwaitResponse(persistable.AwaitableLoopObject):
    PUBLISHER = 'PUBLISHER'
    CORRELATION_ID = 'CORRELATION_ID'
    CONSUMER_TAG = 'CONSUMER_TAG'

    __metaclass__ = abc.ABCMeta

    def __init__(self, publisher, correlation_id):
        super(_AwaitResponse, self).__init__()

        self._publisher = publisher
        self._correlation_id = correlation_id
        self._consumer_tag = None

        self._start_consuming()

    def save_instance_state(self, out_state):
        super(_AwaitResponse, self).save_instance_state(out_state)

        out_state[self.PUBLISHER] = persistable.ObjectProxy(self._publisher)
        out_state[self.CORRELATION_ID] = self._correlation_id
        out_state[self.CONSUMER_TAG] = self._consumer_tag

    def load_instance_state(self, saved_state):
        super(_AwaitResponse, self).load_instance_state(saved_state)

        self._publisher = saved_state[self.PUBLISHER]
        self._correlation_id = saved_state[self.CORRELATION_ID]
        self._consumer_tag = saved_state[self.CONSUMER_TAG]

        self._start_consuming()
        # TODO: Ask the connection to resend messages

    def _start_consuming(self):
        self._consumer_tag = self._publisher._consume_responses(self._response)

    def _stop_consuming(self):
        self._publisher._channel.basic_cancel(self._consumer_tag)

    @abc.abstractmethod
    def on_response(self, ch, method, props, body):
        pass

    def _response(self, ch, method, props, body):
        if props.correlation_id == self._correlation_id:
            self.on_response(ch, method, props, body)


class _AwaitLaunch(_AwaitResponse):
    def on_response(self, ch, method, props, body):
        assert props.correlation_id == self._correlation_id

        pub = self._publisher
        response = pub._response_decode(body)

        if not response['state'] == 'launched':
            self.awaiting().set_exception()

        await_done = self.loop().create(
            _AwaitDone, self._publisher, self._correlation_id)
        self.set_result((response['pid'], await_done))


class _AwaitDone(_AwaitResponse):
    def on_response(self, ch, method, props, body):
        assert props.correlation_id == self._correlation_id

        pub = self._publisher
        response = pub._response_decode(body)
        self.set_result(response)
