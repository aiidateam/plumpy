import collections
from functools import partial
import logging
import plum
import plum.utils
import pika
import sys
import uuid
import yaml

from . import defaults
from . import messages
from . import pubsub
from . import utils

_LOGGER = logging.getLogger(__name__)

__all__ = ['ProcessLaunchSubscriber', 'ProcessLaunchPublisher']

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
LAUNCH_CONTINUE_TASK = 'launch_continue'


class TaskMessage(messages.Message):
    def __init__(self, correlation_id=None):
        self.correlation_id = correlation_id if correlation_id is not None else str(uuid.uuid4())
        self.future = plum.Future()

    def on_delivered(self, publisher):
        publisher.await_response(self.correlation_id, self.on_response)

    def on_delivery_failed(self, publisher, reason):
        self.future.set_exception(
            RuntimeError("Message could not be delivered: {}".format(reason)))

    def on_response(self, done_future):
        plum.copy_future(done_future, self.future)


class SimpleTaskMessage(TaskMessage):
    def __init__(self, body, correlation_id=None):
        super(SimpleTaskMessage, self).__init__(correlation_id)
        self.body = body

    def send(self, publisher):
        if self.correlation_id is None:
            self.correlation_id = str(uuid.uuid4())
        publisher.publish_msg(self.body, None, self.correlation_id)
        return self.future

    def on_delivered(self, publisher):
        publisher.await_response(self.correlation_id, self.on_response)

    def on_delivery_failed(self, publisher, reason):
        self.future.set_exception(
            RuntimeError("Message could not be delivered: {}".format(reason)))

    def on_response(self, done_future):
        plum.copy_future(done_future, self.future)


class LaunchContinueTask(TaskMessage):
    def __init__(self, process_class, init_args, init_kwargs, correlation_id=None):
        super(LaunchContinueTask, self).__init__(correlation_id)
        self._process_class = process_class
        self._init_args = init_args
        self._init_kwargs = init_kwargs

    def send(self, publisher):
        launch = create_launch_task(self._process_class, self._init_args, self._init_kwargs)
        launch.future.add_done_callback(partial(self._on_launch_done, publisher))
        publisher.action_task(launch)
        return self.future

    def on_delivered(self, publisher):
        publisher.await_response(self.correlation_id, self.on_response)

    def on_delivery_failed(self, publisher, reason):
        self.future.set_exception(
            RuntimeError("Message could not be delivered: {}".format(reason)))

    def on_response(self, done_future):
        plum.copy_future(done_future, self.future)

    def _on_launch_done(self, publisher, launch_future):
        if launch_future.cancelled():
            self.future.cancel()
        elif launch_future.exception() is not None:
            self.futrue.set_exception(launch_future.exception())
        else:
            # Action the continue task
            continue_task = create_continue_task(launch_future.result())





def create_launch_task(process_class, init_args=None, init_kwargs=None):
    task = {TASK_KEY: LAUNCH_TASK, PROCESS_CLASS_KEY: plum.utils.class_name(process_class)}
    if init_args:
        task[ARGS_KEY] = init_args
    if init_kwargs:
        task[KWARGS_KEY] = init_kwargs
    return SimpleTaskMessage(task)


def create_continue_task(pid, tag=None):
    task = {TASK_KEY: CONTINUE_TASK, PID_KEY: pid}
    if tag is not None:
        task[TAG_KEY] = tag
    return SimpleTaskMessage(task)


def create_launch_continue_task(process_class, init_args=None, init_kwargs=None):
    return LaunchContinueTask(process_class, init_args, init_kwargs)


class ProcessLaunchSubscriber(messages.BaseConnectionWithExchange):
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
                 task_queue_name=defaults.TASK_QUEUE,
                 testing_mode=False,
                 decoder=yaml.load,
                 encoder=yaml.dump,
                 loop=None,
                 persister=None,
                 unbunble_args=(),
                 unbunble_kwargs=None,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 exchange_params=None,
                 ):
        """
        :param connector: An RMQ connector
        :type connector: :class:`pubsub.RmqConnector`
        :param task_queue_name: The name of the queue to use
        :param decoder: A message decoder
        :param encoder: A response encoder
        :param loop: The event loop
        :param persister: The persister for continuing a process
        :type persister: :class:`plum.Persister`
        :param unbunble_args: Positional arguments passed to saved_state.unbundle
            when continuing a process
        :param unbunble_kwargs: Keyword arguments passed to saved_state.unbundle
            when continuing a process (by default will pass loop)
        """
        super(ProcessLaunchSubscriber, self).__init__(
            connector,
            exchange_name=exchange_name,
            exchange_params=exchange_params
        )

        self._task_queue_name = task_queue_name
        self._testing_mode = testing_mode
        self._decode = decoder
        self._encode = encoder
        self._loop = loop if loop is not None else plum.get_event_loop()
        self._persister = persister
        self._unbundle_args = unbunble_args
        self._unbundle_kwargs = unbunble_kwargs if unbunble_kwargs is not None else {'loop': loop}

    @messages.initialiser()
    def on_channel_open(self, channel):
        super(ProcessLaunchSubscriber, self).on_channel_open(channel)
        channel.basic_qos(prefetch_count=1)

    @messages.initialiser()
    def on_exchange_declareok(self, unused_frame):
        super(ProcessLaunchSubscriber, self).on_exchange_declareok(unused_frame)
        self.get_channel().queue_declare(
            self._on_task_queue_declaredok, queue=self._task_queue_name,
            durable=not self._testing_mode, auto_delete=self._testing_mode)

    @messages.initialiser()
    def _on_task_queue_declaredok(self, frame):
        queue_name = frame.method.queue
        self.get_channel().queue_bind(
            self._on_task_queue_bindok, queue_name, self._exchange_name,
            routing_key=queue_name)

    @messages.initialiser()
    def _on_task_queue_bindok(self, unused_frame):
        self._consumer_tag = \
            self.get_channel().basic_consume(self._on_task, self._task_queue_name)

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
        self._send_response(props.correlation_id, props.reply_to, response)
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

    def _send_response(self, correlation_id, reply_to, response):
        # Build full response
        response[utils.HOST_KEY] = utils.get_host_info()
        self.get_channel().basic_publish(
            exchange='', routing_key=reply_to,
            body=self._encode(response),
            properties=pika.BasicProperties(correlation_id=correlation_id))


class ProcessLaunchPublisher(messages.BasePublisherWithReplyQueue):
    """
    Class used to publishes messages requesting a process to be launched
    """

    def __init__(self, connector,
                 task_queue_name=defaults.TASK_QUEUE,
                 testing_mode=False,
                 exchange_name=defaults.MESSAGE_EXCHANGE,
                 exchange_params=None,
                 encoder=yaml.dump,
                 decoder=yaml.load,
                 confirm_deliveries=True, ):
        super(ProcessLaunchPublisher, self).__init__(
            connector,
            exchange_name=exchange_name,
            exchange_params=exchange_params,
            encoder=encoder,
            decoder=decoder,
            confirm_deliveries=confirm_deliveries
        )
        self._task_queue_name = task_queue_name
        self._testing_mode = testing_mode

    def launch_process(self, process_class, init_args=None, init_kwargs=None):
        """
        Send a request to continue a Process from the provided bundle

        :param process_class: The Process to launch
        :param init_args: positional args for process class constructor
        :param init_kwargs: keyword args for process class constructor
        """
        task = create_launch_task(process_class, init_args, init_kwargs)
        return self.action_message(task)

    def continue_process(self, pid, tag=None):
        task = create_continue_task(pid, tag)
        return self.action_message(task)

    @messages.initialiser()
    def on_exchange_declareok(self, frame):
        super(ProcessLaunchPublisher, self).on_exchange_declareok(frame)

        # The task queue
        self.get_channel().queue_declare(
            self._on_task_queue_declareok,
            self._task_queue_name, durable=not self._testing_mode,
            auto_delete=self._testing_mode)

    def publish_msg(self, task, routing_key, correlation_id):
        if routing_key is not None:
            _LOGGER.warn(
                "Routing key '{}' passed but is ignored for all tasks".format(routing_key))

        properties = pika.BasicProperties(
            reply_to=self.get_reply_queue_name(),
            delivery_mode=2,  # Persistent
            correlation_id=correlation_id
        )
        self._channel.basic_publish(
            exchange=self.get_exchange_name(),
            routing_key=self._task_queue_name,
            body=self._encode(task),
            properties=properties)

    @messages.initialiser()
    def _on_task_queue_declareok(self, frame):
        queue_name = frame.method.queue
        self.get_channel().queue_bind(
            self._on_task_queue_bindok, queue_name, self._exchange_name,
            routing_key=queue_name)

    @messages.initialiser()
    def _on_task_queue_bindok(self, unused_frame):
        pass
