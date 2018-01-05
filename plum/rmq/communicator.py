import abc
import collections
from functools import partial
from future.utils import with_metaclass
import pika
import plum
import uuid
import yaml

from . import defaults
from . import pubsub
from . import utils

__all__ = ['RmqCommunicator']

_RESULT_KEY = 'result'
# This means that the intent has been actioned but not yet completed
_ACTION_SCHEDULED = 'SCHEDULED'
# This means that the intent has been completed
_ACTION_DONE = 'DONE'
# The action failed to be completed
_ACTION_FAILED = 'ACTION_FAILED'


def declare_exchange(channel, name, done_callback):
    channel.exchange_declare(
        done_callback, exchange=name, exchange_type='topic', auto_delete=True)


class Message(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def send(self):
        """
        :return:
        """
        pass

    @abc.abstractmethod
    def on_delivery(self, successful):
        pass


class RpcMessage(Message):
    def __init__(self, communicator, recipient_id, body):
        self.communicator = communicator
        self.recipient_id = recipient_id
        self.body = body
        self.correlation_id = None
        self.future = plum.Future()

    def send(self):
        self.correlation_id = str(uuid.uuid4())
        routing_key = "rpc.{}".format(self.recipient_id)
        self.communicator.publish_msg(self.body, routing_key, self.correlation_id)
        return self.future

    def on_delivery(self, successful):
        if successful:
            self.communicator.await_response(self.correlation_id, self.on_response)
        else:
            self.future.set_exception(RuntimeError("Message could not be delivered"))

    def on_response(self, done_future):
        plum.copy_future(done_future, self.future)


class RmqPublisher(pubsub.ConnectionListener):
    """

    """
    # Bitmasks for starting up the launcher
    EXCHANGE_BOUND = 0b01
    RESPONSE_QUEUE_CREATED = 0b10
    RMQ_INITAILISED = 0b11

    def __init__(self, connector,
                 exchange_name=defaults.CONTROL_EXCHANGE,
                 encoder=yaml.dump,
                 decoder=yaml.load):

        self._exchange_name = exchange_name
        self._encode = encoder
        self._response_decode = decoder
        self._queued_messages = []
        self._reset_channel()

        self._awaiting_response = {}
        connector.add_connection_listener(self)
        if connector.is_connected:
            self._open_channel(connector.connection())

    def initialised_future(self):
        return self._initialising

    def rpc_send(self, recipient_id, msg):
        message = RpcMessage(self, recipient_id, body=msg)
        self._action_message(message)
        return message.future

    def await_response(self, correlation_id, callback):
        self._awaiting_response[correlation_id] = callback

    def publish_msg(self, msg, routing_key, correlation_id=None):
        self._channel.basic_publish(
            exchange=self._exchange_name, routing_key=routing_key,
            properties=pika.BasicProperties(
                reply_to=self._callback_queue_name, correlation_id=correlation_id,
                delivery_mode=1,
                content_type='text/json',
                # expiration="600"
            ),
            body=self._encode(msg),
            mandatory=True
        )

    def on_connection_opened(self, connector, connection):
        self._open_channel(connection)

    def _action_message(self, message):
        if self._initialising.done():
            self._send_message(message)
        else:
            self._queued_messages.append(message)

    def _on_response(self, ch, method, props, body):
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
        message.send()
        self._num_published += 1
        self._sent_messages[self._num_published] = message

    # region RMQ communications
    def _reset_channel(self):
        """ Reset all channel specific members """
        self._callback_queue_name = None
        self._channel = None
        self._num_published = 0
        self._initialisation_state = 0
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
        # Need to confirm delivery so unroutable messages generate a return callback
        channel.confirm_delivery(self._on_delivery_confirmed)
        declare_exchange(channel, self._exchange_name, self._on_exchange_declareok)

        # Declare the response queue
        channel.queue_declare(self._on_queue_declareok, exclusive=True, auto_delete=True)

    def _on_channel_close(self, channel, reply_code, reply_text):
        self._reset_channel()

    def _on_channel_return(self, channel, method, props, body):
        correlation_id = props.correlation_id
        try:
            message = self._sent_messages.pop(method.delivery_tag)
        except ValueError:
            pass
        else:
            message.on_delivery(False)

    def _on_exchange_declareok(self, frame):
        self._initialisation_state |= self.EXCHANGE_BOUND
        if self._initialisation_state == self.RMQ_INITAILISED:
            self._initialising.set_result(True)

    def _on_queue_declareok(self, frame):
        self._callback_queue_name = frame.method.queue
        self._channel.basic_consume(
            self._on_response, no_ack=True, queue=self._callback_queue_name)
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
            message.on_delivery(True)

            # endregion


class RmqSubscriber(pubsub.ConnectionListener):
    # Bitmasks for starting up the launcher
    RPC_QUEUE_CREATED = 0b01
    BROADCAST_QUEUE_CREATED = 0b10
    RMQ_INITAILISED = 0b11

    def __init__(self, connector,
                 exchange_name=defaults.CONTROL_EXCHANGE,
                 decoder=yaml.load,
                 encoder=yaml.dump):
        """
        Subscribes and listens for process control messages and acts on them
        by calling the corresponding methods of the process manager.

        :param connector: The RMQ connector
        :param exchange_name: The name of the exchange to use
        :param decoder:
        """
        self._connector = connector
        self._exchange_name = exchange_name
        self._decode = decoder
        self._response_encode = encoder
        self._channel = None

        self._specific_receivers = {}
        self._all_receivers = []

        self._reset_channel()

        connector.add_connection_listener(self)
        if connector.is_connected:
            self._open_channel(connector.connection())

    def initialised_future(self):
        return self._initialising

    def register_receiver(self, receiver, identifier=None):
        if identifier is not None:
            if not isinstance(identifier, basestring):
                raise TypeError("Identifier must be a unicode or string")
            self._specific_receivers[identifier] = receiver
        self._all_receivers.append(receiver)

    def on_connection_opened(self, connector, connection):
        self._open_channel(connection)

    # region RMQ methods
    def _reset_channel(self):
        self._initialisation_state = 0
        self._initialising = plum.Future()

    def _open_channel(self, connection):
        """ We have a connection, now create a channel """
        self._connector.open_channel(self._on_channel_open)

    def _on_channel_open(self, channel):
        """ We have a channel, now declare the exchange """
        self._channel = channel
        declare_exchange(channel, self._exchange_name, self._on_exchange_declareok)

    def _on_exchange_declareok(self, unused_frame):
        """
        The exchange is up, now create an temporary, exclusive queue for us
        to receive messages on.
        """
        # RPC queue
        self._channel.queue_declare(self._on_rpc_queue_declareok, exclusive=True, auto_delete=True)
        # Broadcast queue
        self._channel.queue_declare(self._on_broadcast_queue_declareok, exclusive=True, auto_delete=True)

    def _on_rpc_queue_declareok(self, frame):
        """
        The queue as been declared, now bind it to the exchange using the
        routing keys we're listening for.
        """
        queue_name = frame.method.queue
        self._channel.queue_bind(
            partial(self._on_rpc_bindok, queue_name), queue_name, self._exchange_name,
            routing_key='rpc.*')

    def _on_rpc_bindok(self, queue_name, unused_frame):
        """ The queue has been bound, we can start consuming. """
        self._channel.basic_consume(self._on_rpc, queue=queue_name)
        self._initialisation_state |= self.RPC_QUEUE_CREATED
        if self._initialisation_state == self.RMQ_INITAILISED:
            self._initialising.set_result(True)

    def _on_broadcast_queue_declareok(self, frame):
        queue_name = frame.method.queue
        self._channel.queue_bind(
            partial(self._on_broadcast_bindok, queue_name), queue_name, self._exchange_name,
            routing_key="broadcast")

    def _on_broadcast_bindok(self, queue_name, unused_frame):
        """ The queue has been bound, we can start consuming. """
        self._channel.basic_consume(self._on_broadcast, queue=queue_name)
        self._initialisation_state |= self.BROADCAST_QUEUE_CREATED
        if self._initialisation_state == self.RMQ_INITAILISED:
            self._initialising.set_result(True)
        # end region

    def _on_rpc(self, ch, method, props, body):
        identifier = method.routing_key[len('rpc.'):]
        receiver = self._specific_receivers.get(identifier, None)
        if receiver is None:
            self._channel.basic_reject(method.delivery_tag)
        else:
            # Tell the sender that we've dealt with it
            self._channel.basic_ack(method.delivery_tag)

            msg = self._decode(body)

            try:
                result = receiver.on_rpc_receive(msg)
                if isinstance(result, plum.Future):
                    response = utils.pending_response()
                else:
                    response = utils.result_response(result)
            except BaseException as e:
                response = utils.exception_response(e)

            self._send_response(ch, props.reply_to, props.correlation_id, response)

    def _on_broadcast(self, ch, method, props, body):
        msg = self._decode(body)
        for receiver in self._all_receivers:
            try:
                receiver.on_broadcast_receive(msg)
            except BaseException:
                # TODO: Log
                pass

    def _send_response(self, ch, reply_to, correlation_id, response):
        ch.basic_publish(
            exchange='', routing_key=reply_to,
            properties=pika.BasicProperties(correlation_id=correlation_id),
            body=self._response_encode(response)
        )


class RmqCommunicator(plum.Communicator):
    def __init__(self, connector,
                 exchange_name=defaults.CONTROL_EXCHANGE,
                 encoder=yaml.dump,
                 decoder=yaml.load):
        self._publisher = RmqPublisher(connector, exchange_name, encoder=encoder, decoder=decoder)
        self._subscriber = RmqSubscriber(connector, exchange_name, encoder=encoder, decoder=decoder)

    def initialised_future(self):
        return plum.gather(self._publisher.initialised_future(), self._subscriber.initialised_future())

    def register_receiver(self, receiver, identifier=None):
        return self._subscriber.register_receiver(receiver, identifier)

    def rpc_send(self, recipient_id, msg):
        """
        Initiate a remote procedure call on a recipient

        :param recipient_id: The recipient identifier
        :param msg: The body of the message
        :return: A future corresponding to the outcome of the call
        """
        return self._publisher.rpc_send(recipient_id, msg)

    def broadcast_msg(self, msg, reply_to=None, correlation_id=None):
        return self._publisher.broadcast_send(msg, reply_to, correlation_id)
