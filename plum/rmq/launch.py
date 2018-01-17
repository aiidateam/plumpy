import logging
import uuid
from functools import partial

import pika
import yaml

import plum
import plum.utils
from plum.rmq.actions import MessageAction
from plum.communications import Action
from . import defaults
from . import messages
from . import pubsub
from . import utils

_LOGGER = logging.getLogger(__name__)

__all__ = ['ProcessLaunchSubscriber', 'ProcessLaunchPublisher']

TASK_KEY = 'task'
PLAY_KEY = 'play'
PERSIST_KEY = 'persist'
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


class TaskRejected(BaseException):
    pass


class ExecuteProcessAction(Action):
    def __init__(self, process_class, init_args=None, init_kwargs=None):
        super(ExecuteProcessAction, self).__init__()
        self._process_class = process_class
        self._init_args = init_args
        self._init_kwargs = init_kwargs

    def execute(self, publisher):
        launch = LaunchProcessAction(
            self._process_class, self._init_args, self._init_kwargs, play=False, persist=True)
        launch.add_done_callback(partial(self._on_launch_done, publisher))
        launch.execute(publisher)

    def _on_launch_done(self, publisher, launch_future):
        if launch_future.cancelled():
            self.cancel()
        elif launch_future.exception() is not None:
            self.set_exception(launch_future.exception())
        else:
            # Action the continue task
            continue_action = ContinueProcessAction(launch_future.result(), play=True)
            plum.chain(continue_action, self)
            continue_action.execute(publisher)


class LaunchProcessAction(MessageAction):
    def __init__(self, *args, **kwargs):
        """
        Calls through to create_launch_body to create the message and so has
        the same signature.
        """
        body = create_launch_body(*args, **kwargs)
        message = TaskMessage(body)
        super(LaunchProcessAction, self).__init__(message)


class ContinueProcessAction(MessageAction):
    def __init__(self, *args, **kwargs):
        """
        Calls through to create_continue_body to create the message and so
        has the same signature.
        """
        body = create_continue_body(*args, **kwargs)
        message = TaskMessage(body)
        super(ContinueProcessAction, self).__init__(message)


class TaskMessage(messages.Message):
    @staticmethod
    def create_launch(process_class, init_args=None, init_kwargs=None, play=True):
        body = create_launch_body(process_class, init_args, init_kwargs, play)
        return TaskMessage(body)

    @staticmethod
    def create_continue(pid, tag=None, play=True):
        body = create_continue_body(pid, tag, play)
        return TaskMessage(body)

    def __init__(self, body, correlation_id=None):
        super(TaskMessage, self).__init__()
        self.correlation_id = correlation_id if correlation_id is not None else str(uuid.uuid4())
        self.body = body
        self.future = plum.Future()

    def send(self, publisher):
        if self.correlation_id is None:
            self.correlation_id = str(uuid.uuid4())
        publisher.publish_msg(self.body, None, self.correlation_id)
        return self.future

    def on_delivered(self, publisher):
        publisher.await_response(self.correlation_id, self.on_response)

    def on_delivery_failed(self, publisher, reason):
        self.future.set_exception(RuntimeError("Message could not be delivered: {}".format(reason)))

    def on_response(self, done_future):
        plum.copy_future(done_future, self.future)


def create_launch_body(process_class, init_args=None, init_kwargs=None, play=True,
                       persist=False):
    msg_body = {
        TASK_KEY: LAUNCH_TASK,
        PROCESS_CLASS_KEY: plum.utils.class_name(process_class),
        PLAY_KEY: play,
        PERSIST_KEY: persist,
    }
    if init_args:
        msg_body[ARGS_KEY] = init_args
    if init_kwargs:
        msg_body[KWARGS_KEY] = init_kwargs
    return msg_body


def create_continue_body(pid, tag=None, play=True):
    msg_body = {
        TASK_KEY: CONTINUE_TASK,
        PID_KEY: pid,
        PLAY_KEY: play,
    }
    if tag is not None:
        msg_body[TAG_KEY] = tag
    return msg_body


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
                result = self._continue(task)
            else:
                raise ValueError("Invalid task type '{}'".format(task_type))

            if isinstance(result, plum.Future):
                result.add_done_callback(partial(self._on_task_done, props, method))
            else:
                # Finished
                self._task_finished(props, method, utils.result_response(result))
        except TaskRejected:
            self._channel.basic_reject(delivery_tag=method.delivery_tag)
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
        if task[PERSIST_KEY] and not self._persister:
            raise TaskRejected("Cannot persist process, no persister")

        proc_class = plum.utils.load_object(task[PROCESS_CLASS_KEY])
        args = task.get(ARGS_KEY, ())
        kwargs = task.get(KWARGS_KEY, {})
        kwargs['loop'] = self._loop
        proc = proc_class(*args, **kwargs)
        if task[PERSIST_KEY]:
            self._persister.save_checkpoint(proc)
        if task[PLAY_KEY]:
            proc.play()
        return proc.pid

    def _continue(self, task):
        if not self._persister:
            raise TaskRejected("Cannot continue process, no persister")

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
        action = LaunchProcessAction(process_class, init_args, init_kwargs)
        action.execute(self)
        return action

    def continue_process(self, pid, tag=None):
        action = ContinueProcessAction(pid, tag)
        action.execute(self)
        return action

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
