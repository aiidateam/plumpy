from collections import namedtuple
import logging
import plum.utils
import pika
import sys
import uuid
import yaml

from . import defaults
from . import pubsub
from . import utils

_LOGGER = logging.getLogger(__name__)

__all__ = ['ProcessLaunchSubscriber', 'ProcessLaunchPublisher']

_RunningTaskInfo = namedtuple("_RunningTaskInfo", ['pid', 'ch', 'delivery_tag'])

TASK_KEY = 'task'
# Launch
PROCESS_CLASS_KEY = 'process_class'
ARGS_KEY = 'args'
KWARGS_KEY = 'kwargs'
# Continue
PID_KEY = 'pid'
TAG_KEY = 'tag'
# Task types
LAUNCH_TASK = 'launch'
CONTINUE_TASK = 'continue'


def create_launch_task(process_class, *args, **kwargs):
    task = {TASK_KEY: LAUNCH_TASK, PROCESS_CLASS_KEY: plum.utils.class_name(process_class)}
    if args:
        task[ARGS_KEY] = args
    if kwargs:
        task[KWARGS_KEY] = kwargs
    return task


def create_continue_task(pid, tag=None):
    task = {TASK_KEY: CONTINUE_TASK, PID_KEY: pid}
    if tag is not None:
        task[TAG_KEY] = tag
    return task


class ProcessLaunchSubscriber(pubsub.ConnectionListener):
    """
    Run tasks as they come form the RabbitMQ task queue.
    Expected format of task:
    For launch:
    {
        'task': [LAUNCH_TASK]
        'process_class': [Process class to launch]
        'args': [tuple of positional args for process constructor]
        'kwargs': [dict of keyword args for process constructor]
    }

    For continue
    {
        'task': [CONTINUE_TASK]
        'pid': [Process ID]
    }
    """

    def __init__(self, connector,
                 queue_name=defaults.TASK_QUEUE,
                 testing_mode=False,
                 decoder=yaml.load,
                 response_encoder=yaml.dump,
                 loop=None,
                 persister=None):
        """
        :param connector: An RMQ connector
        :type connector: :class:`pubsub.RmqConnector`
        :param queue_name: The name of the queue to use
        :param decoder: A message decoder
        :param response_encoder: A response encoder
        :param loop: The event loop
        :param persister: The persister for continuing a process
        :type persister: :class:`plum.Persister`
        """
        self._connector = connector
        self._queue_name = queue_name
        self._testing_mode = testing_mode
        self._decode = decoder
        self._response_encode = response_encoder
        self._loop = loop if loop is not None else plum.get_event_loop()
        self._persister = persister

        # Start RMQ communication
        self._reset_channel()
        connector.add_connection_listener(self)
        if connector.is_connected:
            self._open_channel()

    def on_connection_opened(self, connector, connection):
        self._open_channel()

    def _reset_channel(self):
        self._channel = None

    def _open_channel(self):
        self._connector.open_channel(self._on_channel_open)

    def _on_channel_open(self, channel):
        self._channel = channel
        channel.basic_qos(prefetch_count=1)
        channel.queue_declare(
            self._on_queue_declaredok, queue=self._queue_name,
            durable=not self._testing_mode, auto_delete=self._testing_mode)

    def _on_queue_declaredok(self, frame):
        self._consumer_tag = self._channel.basic_consume(
            self._on_task, self._queue_name)

    def _on_task(self, ch, method, props, body):
        """
        Consumer function that processes the launch message.

        :param ch: The channel
        :param method: The method
        :param props: The message properties
        :param body: The message body
        """
        try:
            task = self._decode(body)
            task_type = task[TASK_KEY]
            if task_type == LAUNCH_TASK:
                response = utils.result_response(self._launch(task))
            elif task_type == CONTINUE_TASK:
                # If we don't have a persister then we reject the message and
                # allow it to be requeued
                if self._persister is None:
                    self._channel.basic_reject(delivery_tag=method.delivery_tag)
                    return
                response = utils.result_response(self._continue(task))
            else:
                raise ValueError("Invalid task type '{}'".format(task_type))
        except KeyboardInterrupt:
            raise
        except Exception as e:
            response = utils.exception_response(e)

        # Send the response
        self._send_response(
            self._channel, props.correlation_id, props.reply_to, response)
        self._channel.basic_ack(delivery_tag=method.delivery_tag)

    def _launch(self, task):
        proc_class = plum.utils.load_object(task[PROCESS_CLASS_KEY])
        args = task.get(ARGS_KEY, ())
        kwargs = task.get(KWARGS_KEY, {})
        kwargs['loop'] = self._loop
        proc = proc_class(*args, **kwargs)
        proc.play()
        return proc.pid

    def _continue(self, task):
        if not self._persister:
            raise RuntimeError("Cannot continue process no persister")
        tag = task.get(TAG_KEY, None)
        saved_state = self._persister.load_checkpoint(task[PID_KEY], tag)
        proc = saved_state.unbundle(self._loop)
        proc.play()
        return True

    def _send_response(self, ch, correlation_id, reply_to, response):
        # Build full response
        full_response = {
            utils.RESPONSE_KEY: response,
            utils.HOST_KEY: utils.get_host_info()
        }
        ch.basic_publish(
            exchange='', routing_key=reply_to,
            body=self._response_encode(full_response),
            properties=pika.BasicProperties(correlation_id=correlation_id))


class ProcessLaunchPublisher(pubsub.ConnectionListener):
    """
    Class used to publishes messages requesting a process to be launched
    """
    TASK_QUEUE_CREATED = 0b01
    RESPONSE_QUEUE_CREATED = 0b10
    ALL_QUEUES_CREATED = 0b11

    def __init__(self, connector,
                 queue_name=defaults.TASK_QUEUE,
                 testing_mode=False,
                 encoder=yaml.dump,
                 response_decoder=yaml.load,
                 loop=None):
        """
        :param connector: An RMQ connector
        :type connector: :class:`pubsub.RmqConnector`
        :param queue_name: The name of the queue to use
        :param encoder: A message encoder
        :param response_decoder: A response encoder
        :param loop: The event loop
        :param persister: The persister for continuing a process
        :type persister: :class:`plum.Persister`
        """
        self._connector = connector
        self._queue_name = queue_name
        self._testing_mode = testing_mode
        self._response_queue_name = None
        self._encode = encoder
        self._response_decode = response_decoder
        self._loop = loop if loop is not None else plum.get_event_loop()

        self._publish_queue = []
        # A mapping of correlation id: future
        self._futures = {}

        # Start RMQ communication
        self._reset_channel()
        connector.add_connection_listener(self)
        if connector.is_connected:
            self._open_channel()

    def on_connection_opened(self, connector, reconnecting):
        self._open_channel()

    def close(self):
        self._connector.remove_connection_listener(self)
        self._connector = None
        self._channel.close()
        self._channel = None

    def launch(self, process_class, *args, **kwargs):
        """
        Send a request to continue a Process from the provided bundle

        :param process_class: The Process to launch
        :param args: positional args for process class constructor
        :param kwargs: keyword args for process class constructor
        """
        task = create_launch_task(process_class, *args, **kwargs)
        return self._action_task(task)

    def continue_process(self, pid, tag=None):
        task = create_continue_task(pid, tag)
        return self._action_task(task)

    def _reset_channel(self):
        self._channel = None
        self._queues_created = 0
        self._publishing = False

    def _open_channel(self):
        self._connector.open_channel(self._on_channel_open)

    def _on_channel_open(self, channel):
        self._channel = channel
        # The task queue
        channel.queue_declare(
            self._on_launch_queue_declareok,
            self._queue_name, durable=not self._testing_mode,
            auto_delete=self._testing_mode)
        # The response queue
        channel.queue_declare(
            self._on_response_queue_declareok, exclusive=True, auto_delete=True)

    def _on_launch_queue_declareok(self, frame):
        self._queues_created |= self.TASK_QUEUE_CREATED
        self._queue_created()

    def _on_response_queue_declareok(self, frame):
        self._response_queue_name = frame.method.queue
        self._channel.basic_consume(self._on_response, no_ack=True)
        self._queues_created |= self.RESPONSE_QUEUE_CREATED
        self._queue_created()

    def _queue_created(self):
        """ Count the number of queues we've opened, when both response and """
        if self._queues_created == self.ALL_QUEUES_CREATED:
            self._publishing = True
            self._publish_queued()

    def _action_task(self, task):
        correlation_id = self._new_correlation_id()
        future = plum.Future()
        if self._publishing:
            # Publish it
            future = plum.Future()
            self._futures[correlation_id] = future
            self._publish_task(task, correlation_id)
        else:
            # Queue it
            self._publish_queue.append((task, correlation_id, future))

        return future

    def _publish_task(self, task, correlation_id):
        properties = pika.BasicProperties(
            reply_to=self._response_queue_name,
            delivery_mode=2,  # Persistent
            correlation_id=correlation_id
        )
        self._channel.basic_publish(
            exchange='', routing_key=self._queue_name, body=self._encode(task),
            properties=properties)

    def _on_response(self, ch, method, props, body):
        try:
            future = self._futures.pop(props.correlation_id)
            response = self._response_decode(body)
            utils.response_to_future(response[utils.RESPONSE_KEY], future)
        except ValueError:
            pass

    def _new_correlation_id(self):
        return str(uuid.uuid4())

    def _publish_queued(self):
        for task, correlation_id, future in self._publish_queue:
            try:
                self._publish_task(task, correlation_id)
                self._futures[correlation_id] = future
            except KeyboardInterrupt:
                raise
            except Exception:
                future.set_exc_info(sys.exc_info())
        self._publish_queue = []
