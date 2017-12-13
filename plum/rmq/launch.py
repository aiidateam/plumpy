import abc
from collections import namedtuple
import logging
import plum.futures
import pika
import traceback
import uuid
import yaml

from future.utils import with_metaclass

from plum import process
from plum.rmq.defaults import Defaults
from . import pubsub

_LOGGER = logging.getLogger(__name__)

__all__ = ['ProcessLaunchSubscriber', 'ProcessLaunchPublisher']

_RunningTaskInfo = namedtuple("_RunningTaskInfo", ['pid', 'ch', 'delivery_tag'])


class ProcessLaunchSubscriber(pubsub.BasicRmqClient):
    """
    Run tasks as they come form the RabbitMQ task queue

    .. warning:: the pika library used is not thread safe and as such the
        connection passed to the constructor must be created on the same thread
        as the start method is called.
    """

    def __init__(self, amqp_url,
                 queue_name=Defaults.TASK_QUEUE,
                 decoder=yaml.load,
                 response_encoder=yaml.dump,
                 loop=None):
        if loop is None:
            loop = plum.get_event_loop()
        super(ProcessLaunchSubscriber, self).__init__(
            amqp_url, auto_reconnect_timeout=5., loop=loop)

        self._queue_name = queue_name

        self._decode = decoder
        self._response_encode = response_encoder

    def _on_channel_open(self, channel):
        super(ProcessLaunchSubscriber, self)._on_channel_open(channel)
        channel.basic_qos(prefetch_count=1)
        channel.queue_declare(self._on_queue_declaredok, queue=self._queue_name)

    def _on_queue_declaredok(self, frame):
        self._consumer_tag = self._channel.basic_consume(
            self._on_launch, self._queue_name
        )

    def _on_launch(self, ch, method, props, body):
        """
        Consumer function that processes the launch message.

        :param ch: The channel
        :param method: The method
        :param props: The message properties
        :param body: The message body
        """
        try:
            message = self._decode(body)
        except KeyboardInterrupt:
            raise
        except Exception:
            response = {
                'state': 'exception',
                'exception': 'Failed to decode task:\n{}'.format(
                    traceback.format_exc()
                )
            }
            self._pubsub.publish_message(
                pika.BasicProperties(correlation_id=props.correlation_id),
                message=self._response_encode(response),
                routing_key=props.reply_to
            )
        else:
            proc_class = message['process_class']
            try:
                proc = proc_class(*proc_class['args'], **proc_class['kwargs'])
                proc.play()
                response = {'state': 'playing'}
            except KeyboardInterrupt:
                raise
            except Exception:
                response = {
                    'state': 'exception',
                    'exception': 'Failed to decode task:\n{}'.format(
                        traceback.format_exc()
                    )
                }
            self._channel.publish_message(
                pika.BasicProperties(correlation_id=props.correlation_id),
                message=self._response_encode(response),
                routing_key=props.reply_to
            )

        self._channel.basic_ack(delivery_tag=method.delivery_tag)


class ProcessLaunchPublisher(pubsub.BasicRmqClient):
    """
    Class used to publishes messages requesting a process to be launched
    """

    def __init__(self, amqp_url,
                 queue_name=Defaults.TASK_QUEUE,
                 encoder=yaml.dump,
                 response_decoder=yaml.load,
                 loop=None):
        if loop is None:
            loop = plum.get_event_loop()
        super(ProcessLaunchSubscriber, self).__init__(
            amqp_url, auto_reconnect_timeout=5., loop=loop)

        self._queue_name = queue_name
        self._response_queue_name = None
        self._encode = encoder
        self._response_decode = response_decoder

        # Response queue
        # result = self._channel.queue_declare(exclusive=True, durable=True)
        # self._callback_queue = result.method.queue
        # self._channel.basic_consume(self._on_response, no_ack=False, queue=self._callback_queue)

    def _on_channel_open(self, channel):
        super(ProcessLaunchPublisher, self)._on_channel_open(channel)
        channel.queue_declare(self._on_launch_queue_declareok,
                              self._queue_name, durable=True)
        channel.queue_declare(self._on_response_queue_declareok,
                              exclusive=True)

    def _on_launch_queue_declareok(self, frame):
        # TODO: Publish all unpublished
        pass

    def _on_response_queue_declareok(self, frame):
        self._response_queue_name = frame.method.queue
        self._channel.basic_consume(
            self._on_response, no_ack=False)

    def launch(self, process_class, *args, **kwargs):
        """
        Send a request to continue a Process from the provided bundle

        :param process_class: The Process to launch
        """

        msg = {
            'process_class': process_class,
            'args': args,
            'kwargs': kwargs
        }

        correlation_id = str(uuid.uuid4())
        properties = pika.BasicProperties(
            # reply_to=publisher._callback_queue,
            delivery_mode=2,
            correlation_id=correlation_id
        )
        return self._publisher.publish_message(properties, msg)

    def close(self):
        close_future = self._publisher.close()
        self._publisher = None
        return close_future
