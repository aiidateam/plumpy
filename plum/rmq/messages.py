import abc
import collections
from future.utils import with_metaclass
import pika
import plum
import uuid
import yaml

from . import pubsub
from . import defaults
from . import utils


class Publisher(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def publish_msg(self, body, routing_key, correlation_id):
        pass

    @abc.abstractmethod
    def await_response(self, correlation_id, callback):
        pass


class Message(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def send(self, publisher):
        """
        :return:
        """
        pass

    @abc.abstractmethod
    def on_delivered(self):
        pass

    @abc.abstractmethod
    def on_delivery_failed(self, reason):
        """
        The delivery of the message failed
        :param reason: Text containng the error
        :type reason: str
        """
        pass


class RpcMessage(Message):
    """
    A Remote Procedure Call message that waits for a response from the recipient.
    """

    def __init__(self, recipient_id, body):
        self.recipient_id = recipient_id
        self.body = body
        self.correlation_id = None
        self.future = plum.Future()
        self._publisher = None

    def send(self, publisher):
        self._publisher = publisher
        self.correlation_id = str(uuid.uuid4())
        routing_key = "rpc.{}".format(self.recipient_id)
        publisher.publish_msg(self.body, routing_key, self.correlation_id)
        return self.future

    def on_delivered(self):
        self._publisher.await_response(self.correlation_id, self.on_response)

    def on_delivery_failed(self, reason):
        self.future.set_exception(
            RuntimeError("Message could not be delivered: {}".format(reason)))

    def on_response(self, done_future):
        plum.copy_future(done_future, self.future)


class BasePublisherWithResponseQueue(pubsub.ConnectionListener, Publisher):
    """

    """
    # Bitmasks for starting up the launcher
    EXCHANGE_BOUND = 0b01
    RESPONSE_QUEUE_CREATED = 0b10
    RMQ_INITAILISED = 0b11

    DEFAULT_EXCHANGE_PARAMS = {
        'exchange_type': 'topic',
        'auto_delete': True
    }

    def __init__(self, connector,
                 exchange_name=defaults.CONTROL_EXCHANGE,
                 exchange_params=None,
                 encoder=yaml.dump,
                 decoder=yaml.load,
                 confirm_deliveries=True):
        if exchange_params is None:
            exchange_params = self.DEFAULT_EXCHANGE_PARAMS

        self._exchange_name = exchange_name
        self._exchange_params = exchange_params
        self._encode = encoder
        self._response_decode = decoder
        self._confirm_deliveries = confirm_deliveries

        self._queued_messages = []
        self._awaiting_response = {}

        self._reset_channel()
        connector.add_connection_listener(self)
        if connector.is_connected:
            self._open_channel(connector.connection())

    def initialised_future(self):
        return self._initialising

    def action_message(self, message):
        if self._initialising.done():
            self._send_message(message)
        else:
            self._queued_messages.append(message)

    def await_response(self, correlation_id, callback):
        self._awaiting_response[correlation_id] = callback

    def publish_msg(self, msg, routing_key, correlation_id):
        self._channel.basic_publish(
            exchange=self._exchange_name, routing_key=routing_key,
            properties=pika.BasicProperties(
                reply_to=self._reply_queue_name, correlation_id=correlation_id,
                delivery_mode=1,
                content_type='text/json',
                # expiration="600"
            ),
            body=self._encode(msg),
            mandatory=True
        )

    def on_connection_opened(self, connector, connection):
        self._open_channel(connection)

    def reply_queue_name(self):
        return self._reply_queue_name

    def _on_response(self, ch, method, props, body):
        """ Called when we get a message on our response queue """
        correlation_id = props.correlation_id
        try:
            callback = self._awaiting_response[correlation_id]
        except IndexError:
            pass
        else:
            response = self._response_decode(body)
            response_future = plum.Future()
            utils.response_to_future(response, response_future)
            if response_future.done():
                self._awaiting_response.pop(correlation_id)
                callback(response_future)
            else:
                pass  # Keep waiting

    def _send_queued_messages(self):
        for msg in self._queued_messages:
            self._send_message(msg)
        self._queued_messages = []

    def _send_message(self, message):
        message.send(self)
        self._num_published += 1
        self._sent_messages[self._num_published] = message

    # region RMQ communications
    def _reset_channel(self):
        """ Reset all channel specific members """
        self._channel = None
        self._reply_queue_name = None
        self._num_published = 0
        self._initialisation_state = 0
        if self._confirm_deliveries:
            self._sent_messages = collections.OrderedDict()
        self._initialising = plum.Future()
        self._initialising.add_done_callback(lambda x: self._send_queued_messages())

    def _open_channel(self, connection):
        # Set up communications
        connection.channel(self._on_channel_open)

    def _on_channel_open(self, channel):
        self._channel = channel
        channel.add_on_close_callback(self._on_channel_close)
        channel.add_on_return_callback(self._on_channel_return)
        if self._confirm_deliveries:
            channel.confirm_delivery(self._on_delivery_confirmed)
        self._declare_exchange(channel, self._exchange_name)

        # Declare the response queue
        channel.queue_declare(self._on_queue_declareok, exclusive=True, auto_delete=True)

    def _on_channel_close(self, channel, reply_code, reply_text):
        self._reset_channel()

    def _declare_exchange(self, channel, name):
        channel.exchange_declare(
            self._on_exchange_declareok, exchange=name, **self._exchange_params)

    def _on_exchange_declareok(self, frame):
        self._initialisation_state |= self.EXCHANGE_BOUND
        if self._initialisation_state == self.RMQ_INITAILISED:
            self._initialising.set_result(True)

    def _on_channel_return(self, channel, method, props, body):
        try:
            message = self._sent_messages.pop(method.delivery_tag)
        except ValueError:
            pass
        else:
            message.on_delivery_failed("Channel returned the message")

    def _on_queue_declareok(self, frame):
        self._reply_queue_name = frame.method.queue
        self._channel.basic_consume(
            self._on_response, no_ack=True, queue=self._reply_queue_name)
        self._initialisation_state |= self.RESPONSE_QUEUE_CREATED
        if self._initialisation_state == self.RMQ_INITAILISED:
            self._initialising.set_result(True)

    def _on_delivery_confirmed(self, frame):
        delivery_tag = frame.method.delivery_tag
        try:
            message = self._sent_messages.pop(delivery_tag)
        except ValueError:
            pass
        else:
            message.on_delivered()

            # endregion
