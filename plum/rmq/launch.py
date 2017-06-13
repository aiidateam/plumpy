import json
import pika
from collections import namedtuple

from plum.loop.object import LoopObject
from plum.process_listener import ProcessListener
from plum.rmq.defaults import Defaults
from plum.rmq.util import Subscriber
from plum.util import override, load_class, fullname

_RunningTaskInfo = namedtuple("_RunningTaskInfo", ['pid', 'ch', 'delivery_tag'])


class ProcessLaunchSubscriber(LoopObject, ProcessListener):
    """
    Run tasks as they come form the RabbitMQ task queue

    .. warning:: the pika library used is not thread safe and as such the
        connection passed to the constructor must be created on the same thread
        as the start method is called.
    """

    def __init__(self, connection, queue=Defaults.TASK_QUEUE, decoder=json.loads):
        """
        :param connection: The pika RabbitMQ connection
        :type connection: :class:`pika.Connection`
        :param queue: The queue name to use
        :param decoder: A function to deserialise incoming messages
        """
        super(ProcessLaunchSubscriber, self).__init__()

        self._decode = decoder
        self._running_processes = {}
        self._stopping = False
        self._num_processes = 0

        # Set up communications
        self._channel = connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._channel.queue_declare(queue=queue, durable=True)
        self._channel.basic_consume(self._on_launch, queue=queue)

    @override
    def tick(self, time_limit=1.0):
        """
        Poll the channel for launch process events

        :param time_limit: How long to poll for
        :type time_limit: float
        :return: The number of launch events consumed
        :rtype: int
        """
        self._num_processes = 0
        self._channel.connection.process_data_events(time_limit=time_limit)
        return self._num_processes

    def _on_launch(self, ch, method, properties, body):
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

    def on_process_terminate(self, process):
        """
        A process has finished for whatever reason so clean up.
        
        :param process: The process that terminated
        :type process: :class:`plum.process.Process`
        """
        info = self._running_processes.pop(process.pid)
        info.ch.basic_ack(delivery_tag=info.delivery_tag)


class ProcessLaunchPublisher(object):
    def __init__(self, connection, queue=Defaults.TASK_QUEUE,
                 encoder=json.dumps):
        self._queue = queue
        self._encode = encoder

        self._channel = connection.channel()
        self._channel.queue_declare(queue=queue, durable=True)

    def launch(self, proc_class, inputs=None):
        """
        Send a Process task to be executed by a runner.

        :param proc_class: The Process class to run
        :param inputs: The inputs to supply
        """
        task = {'proc_class': fullname(proc_class),
                'inputs': inputs}
        self._channel.basic_publish(
            exchange='', routing_key=self._queue, body=self._encode(task),
            properties=pika.BasicProperties(delivery_mode=2)  # Persist
        )
