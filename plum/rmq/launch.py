from collections import namedtuple
import json
import pika
import uuid

from plum.loop.object import Ticking, LoopObject
from plum.process_listener import ProcessListener
from plum.rmq.defaults import Defaults
from plum.util import override, load_class, fullname

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


class ProcessLaunchSubscriber(Ticking, LoopObject, ProcessListener):
    """
    Run tasks as they come form the RabbitMQ task queue

    .. warning:: the pika library used is not thread safe and as such the
        connection passed to the constructor must be created on the same thread
        as the start method is called.
    """

    def __init__(self, connection, queue=Defaults.TASK_QUEUE, decoder=json.loads,
                 response_encoder=launched_encode):
        """
        :param connection: The pika RabbitMQ connection
        :type connection: :class:`pika.Connection`
        :param queue: The queue name to use
        :param decoder: A function to deserialise incoming messages
        """
        super(ProcessLaunchSubscriber, self).__init__()

        self._decode = decoder
        self._response_encode = response_encoder
        self._running_processes = {}
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
        self._channel.connection.process_data_events(time_limit=0.1)

    def _on_launch(self, ch, method, props, body):
        """
        Consumer function that processes the launch message.

        :param ch: The channel
        :param method: The method
        :param properties: The message properties
        :param body: The message body
        """
        self._num_processes += 1

        task = self._decode(body)
        proc_class = load_class(task['proc_class'])

        proc = proc_class.new(inputs=task['inputs'])
        proc.add_process_listener(self)

        self._running_processes[proc.pid] = _RunningTaskInfo(proc.pid, ch, method.delivery_tag)
        self.loop().insert(proc)

        # Tell the sender that we've launched it
        ch.basic_publish(
            exchange='', routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
            body=self._response_encode({'pid': proc.pid})
        )

    def on_process_terminate(self, process):
        """
        A process has finished for whatever reason so clean up.
        
        :param process: The process that terminated
        :type process: :class:`plum.process.Process`
        """
        info = self._running_processes.pop(process.pid)
        info.ch.basic_ack(delivery_tag=info.delivery_tag)


class ProcessLaunchPublisher(Ticking, LoopObject):
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
        Send a Process task to be executed by a runner.

        :param proc_class: The Process class to run
        :param inputs: The inputs to supply
        """
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
            return None

        future = None
        if self.loop() is not None:
            future = self.loop().create_future()
            self._responses[correlation_id] = future

        return future

    @override
    def tick(self):
        self._channel.connection.process_data_events(time_limit=0.1)

    def _on_response(self, ch, method, props, body):
        if props.correlation_id in self._responses:
            future = self._responses.pop(props.correlation_id)
            future.set_result(self._response_decode(body))
