

import pika
import json
from collections import namedtuple
from plum.process import ProcessListener
from plum.process_manager import ProcessManager
from plum.util import load_class, fullname


ProcessTask = namedtuple("ProcessTask", ['proc_class', 'inputs'])
_RunningTaskInfo = namedtuple("_RunningTaskInfo", ['pid', 'ch', 'delivery_tag'])


def json_encode(task):
    """
    Take a task and convert it to a JSON string.

    :param task: The task to encode
    :type task: :class:`ProcessTask`
    :return: The encoded task as a JSON string
    :rtype: str
    """
    d = {'proc_class': fullname(task.proc_class),
         'inputs': task.inputs}
    return json.dumps(d)


def json_decode(msg):
    """
    Decodes a JSON dictionary which as the format:
    {
        'proc_class': [class_string]
        'inputs': [None or a dictionary of inputs]
    }


    :param msg: The message to decode
    :return: The task in the format of a ProcessTask tuple
    :rtype: :class:`ProcessTask`
    """
    d = json.loads(msg)
    return ProcessTask(load_class(d['proc_class']), d['inputs'])


class TaskRunner(ProcessListener):
    """
    Run tasks as they come form the RabbitMQ queue and sent by the TaskSender

    .. warning:: the pika library used is not thread safe and as such the
        connection passed to the constructor must be created on the same thread
        as the start method is called.
    """

    def __init__(self, connection, queue='task_queue', decoder=json_decode,
                 manager=None):
        """

        :param connection: The pika RabbitMQ connection
        :type connection: :class:`pika.Connection`
        :param queue: The queue name to use
        :param decoder: A function to deserialise incoming messages
        :param manager: The process manager to use, one will be created if None
            is passed
        :type manager: :class:`plum.process_manger.ProcessManager`
        """
        if manager is None:
            self._manager = ProcessManager()
        else:
            self._manager = manager
        self._decode = decoder
        self._running_processes = {}
        self._stopping = False

        self._channel = connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(self._new_task, queue=queue)

    def start(self):
        while self._channel._consumer_infos:
            self._channel.connection.process_data_events(time_limit=0.2)
            if self._stopping:
                self._channel.stop_consuming()
                self._stopping = False

    def stop(self):
        self._stopping = True
        self._manager.pause_all()

    def _new_task(self, ch, method, properties, body):
        task = self._decode(body)

        p = task.proc_class.new_instance(inputs=task.inputs)
        p.add_process_listener(self)

        self._running_processes[p.pid] = \
            _RunningTaskInfo(p.pid, ch, method.delivery_tag)
        self._manager.start(p)

    # From ProcessListener #################################
    def on_process_stop(self, process):
        self._process_terminated(process)

    def on_process_fail(self, process):
        self._process_terminated(process)
    ########################################################

    def _process_terminated(self, process):
        """
        A process has finished for whatever reason so clean up.
        :param process: The process that finished
        :type process: :class:`plum.process.Process`
        """
        info = self._running_processes.pop(process.pid)
        info.ch.basic_ack(delivery_tag=info.delivery_tag)


class TaskSender(object):

    def __init__(self, connection, queue='task_queue',
                 encoder=json_encode):
        self._queue = queue
        self._encode = encoder

        self._channel = connection.channel()
        self._channel.queue_declare(queue=queue, durable=True)

    def send(self, proc_class, inputs=None):
        """
        Send a Process task to be executed by a runner.

        :param proc_class: The Process class to rurn
        :param inputs: The inputs to supply
        """
        task = ProcessTask(proc_class, inputs)
        self._channel.basic_publish(
            exchange='', routing_key=self._queue, body=self._encode(task),
            properties=pika.BasicProperties(delivery_mode=2) # Persist
        )