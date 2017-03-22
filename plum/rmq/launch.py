import json
import pika
import uuid
from collections import namedtuple

from plum.process_listener import ProcessListener
from plum.process_manager import ProcessManager
from plum.rmq.defaults import Defaults
from plum.rmq.util import Subscriber
from plum.util import override, load_class, fullname

_RunningTaskInfo = namedtuple("_RunningTaskInfo", ['pid', 'ch', 'delivery_tag'])


# TODO: Get rid of this class or change it - not needed
class TaskRunner(Subscriber, ProcessListener):
    """
    Run tasks as they come form the RabbitMQ task queue as sent by the launcher

    .. warning:: the pika library used is not thread safe and as such the
        connection passed to the constructor must be created on the same thread
        as the start method is called.
    """

    def __init__(self, connection, queue=Defaults.TASK_QUEUE,
                 decoder=json.loads, manager=None, controller=None,
                 status_provider=None):
        """

        :param connection: The pika RabbitMQ connection
        :type connection: :class:`pika.Connection`
        :param queue: The queue name to use
        :param decoder: A function to deserialise incoming messages
        :param manager: The process manager to use, one will be created if None is passed
        :type manager: :class:`plum.process_manger.ProcessManager`
        """
        if manager is None:
            self._manager = ProcessManager()
        else:
            self._manager = manager

        if controller is None:
            from plum.rmq.control import ProcessControlSubscriber
            self._controller = ProcessControlSubscriber(connection)
        else:
            self._controller = controller
        self._controller.set_process_manager(self._manager)

        if status_provider is None:
            from plum.rmq.status import ProcessStatusSubscriber
            self._status_provider = ProcessStatusSubscriber(connection)
        else:
            self._status_provider = status_provider
        self._status_provider.set_process_manager(self._manager)

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
    def start(self, poll_time=1.0):
        """
        Start polling for tasks.  Will block until stop() is called.

        .. warning:: Must be called from the same threads where the connection
            that was passed into the constructor was created.
        """
        while self._channel._consumer_infos:
            self.poll(poll_time)
            if self._stopping:
                self._channel.stop_consuming()
                self._stopping = False

    @override
    def poll(self, time_limit=1.0):
        self._num_processes = 0
        self._channel.connection.process_data_events(time_limit=time_limit)
        self._controller.poll(time_limit)
        self._status_provider.poll(time_limit)
        return self._num_processes

    @override
    def stop(self):
        self._stopping = True
        self._manager.pause_all()

    @override
    def shutdown(self):
        self._channel.close()

    def _on_launch(self, ch, method, properties, body):
        self._num_processes += 1

        task = self._decode(body)
        ProcClass = load_class(task['proc_class'])

        p = ProcClass.new(inputs=task['inputs'])
        p.add_process_listener(self)

        self._running_processes[p.pid] = _RunningTaskInfo(p.pid, ch, method.delivery_tag)
        self._manager.start(p)

    # region From ProcessListener
    def on_process_done_playing(self, process):
        self._process_terminated(process)

    # endregion

    def _process_terminated(self, process):
        """
        A process has finished for whatever reason so clean up.
        :param process: The process that finished
        :type process: :class:`plum.process.Process`
        """
        info = self._running_processes.pop(process.pid)
        info.ch.basic_ack(delivery_tag=info.delivery_tag)


class ProcessLaunchSubscriber(Subscriber, ProcessListener):
    """
    Run tasks as they come form the RabbitMQ task queue

    .. warning:: the pika library used is not thread safe and as such the
        connection passed to the constructor must be created on the same thread
        as the start method is called.
    """

    def __init__(self, connection, queue=Defaults.TASK_QUEUE,
                 decoder=json.loads, manager=None):
        """
        :param connection: The pika RabbitMQ connection
        :type connection: :class:`pika.Connection`
        :param queue: The queue name to use
        :param decoder: A function to deserialise incoming messages
        :param manager: The process manager to use, one will be created if None is passed
        :type manager: :class:`plum.process_manger.ProcessManager`
        """
        if manager is None:
            self._manager = ProcessManager()
        else:
            self._manager = manager

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
    def start(self, poll_time=1.0):
        """
        Start polling for tasks.  Will block until stop() is called.

        .. warning:: Must be called from the same threads where the connection
            that was passed into the constructor was created.
        """
        while self._channel._consumer_infos:
            self.poll(poll_time)
            if self._stopping:
                self._channel.stop_consuming()
                self._stopping = False

    @override
    def poll(self, time_limit=1.0):
        self._num_processes = 0
        self._channel.connection.process_data_events(time_limit=time_limit)
        return self._num_processes

    @override
    def stop(self):
        self._stopping = True
        self._manager.pause_all()
        self._status_publisher.reset()

    @override
    def shutdown(self):
        self._channel.close()

    def _on_launch(self, ch, method, properties, body):
        self._num_processes += 1

        task = self._decode(body)
        proc_class = load_class(task['proc_class'])

        p = proc_class.new(inputs=task['inputs'])
        p.add_process_listener(self)

        self._running_processes[p.pid] = _RunningTaskInfo(p.pid, ch, method.delivery_tag)
        self._manager.start(p)

    # region From ProcessListener
    def on_process_done_playing(self, process):
        self._process_terminated(process)

    # endregion

    def _process_terminated(self, process):
        """
        A process has finished for whatever reason so clean up.
        :param process: The process that finished
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
