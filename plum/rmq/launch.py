import collections
from collections import namedtuple
from functools import partial
import logging
import plum
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


def create_launch_task(process_class, init_args=None, init_kwargs=None):
    task = {TASK_KEY: LAUNCH_TASK, PROCESS_CLASS_KEY: plum.utils.class_name(process_class)}
    if init_args:
        task[ARGS_KEY] = init_args
    if init_kwargs:
        task[KWARGS_KEY] = init_kwargs
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
                 persister=None,
                 unbunble_args=(),
                 unbunble_kwargs=None):
        """
        :param connector: An RMQ connector
        :type connector: :class:`pubsub.RmqConnector`
        :param queue_name: The name of the queue to use
        :param decoder: A message decoder
        :param response_encoder: A response encoder
        :param loop: The event loop
        :param persister: The persister for continuing a process
        :type persister: :class:`plum.Persister`
        :param unbunble_args: Positional arguments passed to saved_state.unbundle
            when continuing a process
        :param unbunble_kwargs: Keyword arguments passed to saved_state.unbundle
            when continuing a process (by default will pass loop)
        """
        self._connector = connector
        self._queue_name = queue_name
        self._testing_mode = testing_mode
        self._decode = decoder
        self._response_encode = response_encoder
        self._loop = loop if loop is not None else plum.get_event_loop()
        self._persister = persister
        self._unbundle_args = unbunble_args
        self._unbundle_kwargs = unbunble_kwargs if unbunble_kwargs is not None else {'loop': loop}

        # Start RMQ communication
        self._reset_channel()
        connector.add_connection_listener(self)
        if connector.is_connected:
            self._open_channel()

    def on_connection_opened(self, connector, connection):
        self._open_channel()

    def initialised_future(self):
        return self._initialised_future

    def close(self):
        self._connector.remove_connection_listener(self)
        self._connector = None
        self._channel.close()
        self._channel = None
        self._initialised_future = None

    def _reset_channel(self):
        self._channel = None
        self._initialised_future = plum.Future()

    def _open_channel(self):
        self._connector.open_channel(self._on_channel_open)

    def _on_channel_open(self, channel):
        self._channel = channel
        channel.basic_qos(prefetch_count=1)
        channel.queue_declare(
            self._on_queue_declaredok, queue=self._queue_name,
            durable=not self._testing_mode, auto_delete=self._testing_mode)

    def _on_queue_declaredok(self, frame):
        self._consumer_tag = \
            self._channel.basic_consume(self._on_task, self._queue_name)
        self._initialised_future.set_result(True)

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
                result = self._launch(task)
            elif task_type == CONTINUE_TASK:
                # If we don't have a persister then we reject the message and
                # allow it to be requeued, this way another launcher can potentially
                # deal with it
                if self._persister is None:
                    self._channel.basic_reject(delivery_tag=method.delivery_tag)
                    return
                result = self._continue(task)
            else:
                raise ValueError("Invalid task type '{}'".format(task_type))

            if isinstance(result, plum.Future):
                result.add_done_callback(partial(self._on_task_done, props, method))
            else:
                # Finished
                self._task_finished(props, method, utils.result_response(result))
        except KeyboardInterrupt:
            raise
        except Exception as e:
            self._task_finished(props, method, utils.exception_response(e))

    def _on_task_done(self, props, method, future):
        try:
            response = utils.result_response(future.result())
        except Exception as e:
            response = utils.exception_response(e)
        self._task_finished(props, method, response)

    def _task_finished(self, props, method, response):
        """
        Send an acknowledgement of the task being actioned and a response to the
        initiator.

        :param props: The message properties
        :param method: The message method
        :param response: The response to send to the initiator
        """
        self._send_response(self._channel, props.correlation_id, props.reply_to, response)
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
            raise RuntimeError("Cannot continue process, no persister")
        tag = task.get(TAG_KEY, None)
        saved_state = self._persister.load_checkpoint(task[PID_KEY], tag)
        proc = saved_state.unbundle(*self._unbundle_args, **self._unbundle_kwargs)
        proc.play()
        return proc.future()

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


class TaskInfo(object):
    delivery_confirmed = False
    published_callback = None

    def __init__(self, task, correlation_id):
        self.task = task
        self.correlation_id = correlation_id
        self.future = plum.Future()
        self.publish_future = plum.Future()


class ProcessLaunchPublisher(pubsub.ConnectionListener):
    """
    Class used to publishes messages requesting a process to be launched
    """
    # Bitmasks for starting up the launcher
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
        """
        self._connector = connector
        self._queue_name = queue_name
        self._testing_mode = testing_mode
        self._response_queue_name = None
        self._encode = encoder
        self._response_decode = response_decoder
        self._loop = loop if loop is not None else plum.get_event_loop()

        self._publish_queue = []
        # The list of TaskInfo objects are they were sent
        self._task_info = collections.OrderedDict()
        self._num_tasks = 0

        # Start RMQ communication
        self._reset_channel()
        connector.add_connection_listener(self)
        if connector.is_connected:
            self._open_channel()

    def on_connection_opened(self, connector, reconnecting):
        self._open_channel()

    def close(self):
        self._channel.close()
        self._initialised_future = None
        self._channel = None
        self._connector.remove_connection_listener(self)
        self._connector = None

    def launch_process(self, process_class, init_args=None, init_kwargs=None,
                       published_callback=None):
        """
        Send a request to continue a Process from the provided bundle

        :param process_class: The Process to launch
        :param init_args: positional args for process class constructor
        :param init_kwargs: keyword args for process class constructor
        :param published_callback: A callback function called when the launch
            task has been received by the broker
        """
        task = create_launch_task(process_class, init_args, init_kwargs)
        return self._action_task(task, published_callback)

    def continue_process(self, pid, tag=None, published_callback=None):
        task = create_continue_task(pid, tag)
        return self._action_task(task, published_callback)

    def initialised_future(self):
        return self._initialised_future

    def _reset_channel(self):
        self._initialised_future = plum.Future()
        self._initialised_future.add_done_callback(self._on_ready)
        self._channel = None
        self._queues_created = 0

    def _open_channel(self):
        self._connector.open_channel(self._on_channel_open)

    def _on_channel_open(self, channel):
        self._channel = channel
        channel.confirm_delivery(self._delivery_confirmed)
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
        """ When all queues have been declared we are initialised """
        if self._queues_created == self.ALL_QUEUES_CREATED:
            self._initialised_future.set_result(True)

    def _action_task(self, task, published_callback=None):
        correlation_id = self._new_correlation_id()
        self._num_tasks += 1
        task_info = TaskInfo(task, correlation_id)
        task_info.publish_future.add_done_callback(lambda x: published_callback(task_info.future))

        seq_no = self._num_tasks
        self._task_info[self._num_tasks] = task_info
        if self._publishing:
            # Publish it
            self._publish_task(task_info.task, correlation_id)
        else:
            # Queue it
            self._publish_queue.append(seq_no)

        return task_info.future

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
            seq_no, task_info = self._get_task_info(props.correlation_id)
            response = self._response_decode(body)
            utils.response_to_future(response[utils.RESPONSE_KEY], task_info.future)
            del self._task_info[seq_no]
        except IndexError:
            pass

    def _new_correlation_id(self):
        return str(uuid.uuid4())

    def _publish_queued(self):
        for i in self._publish_queue:
            try:
                task_info = self._task_info[i]
                try:
                    self._publish_task(task_info.task, task_info.correlation_id)
                except KeyboardInterrupt:
                    raise
                except Exception:
                    task_info.future.set_exc_info(sys.exc_info())
            except IndexError:
                _LOGGER.error(
                    "The task info for queued task '{}' could not be found".format(i))
        self._publish_queue = []

    def _on_ready(self, future):
        if future.exception():
            self.close()
        self._publish_queued()

    def _delivery_confirmed(self, frame):
        for seq_no, task_info in self._task_info.items():
            if seq_no == frame.method.delivery_tag or frame.method.multiple:
                if not task_info.publish_future.done():
                    task_info.publish_future.set_result(True)

            if seq_no == frame.method.delivery_tag:
                break

    def _get_task_info(self, correlation_id):
        for seq_no, task_info in self._task_info.items():
            if task_info.correlation_id == correlation_id:
                return seq_no, task_info

    @property
    def _publishing(self):
        return self._initialised_future.done() and self._initialised_future.result()
