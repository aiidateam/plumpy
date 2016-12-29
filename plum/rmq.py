
import pika
import json
import uuid
from collections import namedtuple
from plum.process import ProcessListener
from plum.process_manager import ProcessManager
from plum.util import load_class, fullname


_RunningTaskInfo = namedtuple("_RunningTaskInfo", ['pid', 'ch', 'delivery_tag'])


class Defaults(object):
    TASK_QUEUE = 'plum.task_queue'
    TASK_CONTROL_EXCHANGE = 'plum.task_control'
    STATUS_EXCHANGE = 'plum.status_updates'


class ProcessStatusPublisher(ProcessListener):
    """
    This class publishes status updates from processes based on receiving event
    messages.
    """
    def __init__(self, connection, exchange=Defaults.STATUS_EXCHANGE,
                 encoder=json.dumps):
        self._exchange = exchange
        self._encode = encoder
        self._processes = []

        self._channel = connection.channel()
        self._channel.exchange_declare(
            exchange=self._exchange, type='topic')

    def add_process(self, process):
        """
        Add a process to have its status updates be published

        :param process: The process to publish updates for
        :type process: :class:`plum.process.Process`
        """
        self._processes.append(process)
        process.add_process_listener(self)

    def remove_process(self, process):
        """
        Remove a process from having its status updates be published

        :param process: The process to stop publishing updates for
        :type process: :class:`plum.process.Process`
        """
        process.remove_process_listener(self)
        self._processes.remove(process)

    def reset(self):
        """
        Stop listening to all processes.
        """
        for p in self._processes:
            p.remove_process_listener(self)
        self._processes = []

    # From ProcessListener ####################################################
    def on_process_start(self, process):
        key = "{}.start".format(process.pid)
        d = {'type': fullname(process)}
        self._channel.basic_publish(
            self._exchange, key, body=self._encode(d))

    def on_process_run(self, process):
        key = "{}.run".format(process.pid)
        self._channel.basic_publish(
            self._exchange, key, body="")

    def on_process_wait(self, process):
        key = "{}.wait".format(process.pid)
        self._channel.basic_publish(
            self._exchange, key, body="")

    def on_process_resume(self, process):
        key = "{}.resume".format(process.pid)
        self._channel.basic_publish(
            self._exchange, key, body="")

    def on_process_finish(self, process):
        key = "{}.finish".format(process.pid)
        self._channel.basic_publish(
            self._exchange, key, body="")

    def on_process_stop(self, process):
        key = "{}.stop".format(process.pid)
        self._channel.basic_publish(
            self._exchange, key, body="")
        self.remove_process(process)

    def on_process_fail(self, process):
        key = "{}.fail".format(process.pid)
        exception = process.get_exception()
        d = {'exception_type': fullname(exception),
             'exception_msg': exception.message}
        self._channel.basic_publish(
            self._exchange, key, body=self._encode(d))
        self.remove_process(process)

    def on_output_emitted(self, process, output_port, value, dynamic):
        key = "{}.emitted".format(process.pid)
        # Don't send the value, it could be large and/or unserialisable
        d = {'port': output_port,
             'dynamic': dynamic}
        self._channel.basic_publish(
            self._exchange, key, body=self._encode(d))
    ###########################################################################


class Action(object):
    PLAY = 'play'
    PAUSE = 'pause'
    ABORT = 'abort'


def action_decode(msg):
    d = json.loads(msg)
    try:
        d['pid'] = uuid.UUID(d['pid'])
    except ValueError:
        pass
    return d


def action_encode(msg):
    d = msg.copy()
    if isinstance(d['pid'], uuid.UUID):
        d['pid'] = str(d['pid'])
    return json.dumps(d)


class ProcessController():
    def __init__(self, connection, exchange=Defaults.TASK_CONTROL_EXCHANGE,
                 decoder=action_decode, process_manager=None):
        """
        Create the process controller.

        :param connection:
        :param exchange:
        :param decoder:
        :param process_manager: The process manager running the processes
            that are being controlled.
        :type process_manager: :class:`plum.process_manager.ProcessManager`
        """
        self._decode = decoder
        self._manager = process_manager
        self._stopping = False

        # Set up communications
        self._channel = connection.channel()
        self._channel.exchange_declare(exchange=exchange ,type='fanout')
        result = self._channel.queue_declare(exclusive=True)
        self._queue_name = result.method.queue
        self._channel.queue_bind(exchange=exchange, queue=self._queue_name)
        self._channel.basic_consume(
            self._control_msg, queue=self._queue_name)

    def start(self):
        while self._channel._consumer_infos:
            self.poll()
            if self._stopping:
                self._channel.stop_consuming()
                self._stopping = False

    def poll(self, time_limit=0.2):
        self._channel.connection.process_data_events(time_limit=time_limit)

    def stop(self):
        self._stopping = True

    def set_process_manager(self, manager):
        self._manager = manager

    def _control_msg(self, ch, method, properties, body):
        if self._manager is None:
            return

        d = self._decode(body)
        try:
            intent = d['intent']
            if intent == Action.PLAY:
                self._manager.play(d['pid'])
                ch.basic_ack(delivery_tag=method.delivery_tag)
            elif intent == Action.PAUSE:
                self._manager.pause(d['pid'])
                ch.basic_ack(delivery_tag=method.delivery_tag)
            elif intent == Action.ABORT:
                self._manager.abort(d['pid'])
                ch.basic_ack(delivery_tag=method.delivery_tag)
        except ValueError:
            # The PID isn't known to the process manager
            pass


class TaskRunner(ProcessListener):
    """
    Run tasks as they come form the RabbitMQ queue and sent by the TaskSender

    .. warning:: the pika library used is not thread safe and as such the
        connection passed to the constructor must be created on the same thread
        as the start method is called.
    """

    def __init__(self, connection, queue=Defaults.TASK_QUEUE,
                 decoder=json.loads, manager=None, controller=None):
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

        if controller is None:
            self._controller = ProcessController(connection)
        else:
            self._controller = controller
        self._controller.set_process_manager(self._manager)

        self._decode = decoder
        self._running_processes = {}
        self._stopping = False

        self._status_publisher = ProcessStatusPublisher(connection)

        # Set up communications
        self._channel = connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._channel.queue_declare(queue=queue, durable=True)
        self._channel.basic_consume(self._new_task, queue=queue)

    def start(self):
        """
        Start polling for tasks.  Will block until stop() is called.

        .. warning:: Must be called from the same threads where the connection
            that was passed into the constructor was created.
        """
        while self._channel._consumer_infos:
            self.poll()
            if self._stopping:
                self._channel.stop_consuming()
                self._stopping = False

    def poll(self, time_limit=0.2):
        self._channel.connection.process_data_events(time_limit=time_limit)
        self._controller.poll(time_limit)

    def stop(self):
        self._stopping = True
        self._manager.pause_all()
        self._status_publisher.reset()

    def _new_task(self, ch, method, properties, body):
        task = self._decode(body)
        ProcClass = load_class(task['proc_class'])

        p = ProcClass.new_instance(inputs=task['inputs'])
        p.add_process_listener(self)
        self._status_publisher.add_process(p)

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


class TaskController(object):

    def __init__(self, connection, queue=Defaults.TASK_QUEUE,
                 encoder=json.dumps):
        self._queue = queue
        self._encode = encoder

        self._channel = connection.channel()
        self._channel.queue_declare(queue=queue, durable=True)

    def send(self, proc_class, inputs=None):
        """
        Send a Process task to be executed by a runner.

        :param proc_class: The Process class to run
        :param inputs: The inputs to supply
        """
        task = {'proc_class': fullname(proc_class),
                'inputs': inputs}
        self._channel.basic_publish(
            exchange='', routing_key=self._queue, body=self._encode(task),
            properties=pika.BasicProperties(delivery_mode=2) # Persist
        )