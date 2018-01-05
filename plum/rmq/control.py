import logging
import pika
import pika.exceptions
import uuid
import yaml

import plum
from plum.rmq.utils import add_host_info
from . import defaults
from . import pubsub

__all__ = ['ProcessControlPublisher', 'RmqProcessController']

LOGGER = logging.getLogger(__name__)


class Action(object):
    PLAY = 'play'
    PAUSE = 'pause'
    CANCEL = 'cancel'


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


class ProcessControlPublisher(pubsub.ConnectionListener):
    """
    This class is responsible for sending control messages to processes e.g.
    play, pause, abort, etc.

    The publisher has two RMQ interactions, the control publications:

    P ---- Exchange [fanout] --[queue]--

    and the response queue

    P --[exclusive queue]-- Exchange [default]
    """

    def __init__(self, connector,
                 exchange_name=defaults.CONTROL_EXCHANGE,
                 encoder=yaml.dump,
                 response_decoder=yaml.load):

        self._exchange_name = exchange_name
        self._encode = encoder
        self._response_decode = response_decoder
        self._queued_messages = []
        self._reset_channel()

        connector.add_connection_listener(self)
        if connector.is_connected:
            self._open_channel(connector.connection())

    # region control methods

    def cancel_process(self, pid, msg=None):
        future = plum.Future()
        message = {'pid': pid, 'intent': Action.CANCEL, 'msg': msg}
        if self._channel is not None and self._channel.is_open:
            self._send_msg(message, future)
        else:
            self._queued_messages.append((message, future))
        return future

    def pause_process(self, pid):
        future = plum.Future()
        message = {'pid': pid, 'intent': Action.PAUSE}
        if self._channel is not None and self._channel.is_open:
            self._send_msg(message, future)
        else:
            self._queued_messages.append((message, future))
        return future

    def play_process(self, pid):
        future = plum.Future()
        message = {'pid': pid, 'intent': Action.PLAY}
        if self._channel is not None and self._channel.is_open:
            self._send_msg(message, future)
        else:
            self._queued_messages.append((message, future))
        return future

    # endregion

    def on_connection_opened(self, connector, connection):
        self._open_channel(connection)

    def _open_channel(self, connection):
        # Set up communications
        connection.channel(self._on_channel_open)

    def _reset_channel(self):
        """ Reset all channel specific members """
        self._responses = {}
        self._callback_queue_name = None
        self._channel = None
        self._exchange_bound = False
        self._num_published = 0

    def _on_channel_open(self, channel):
        self._channel = channel
        channel.add_on_close_callback(self._on_channel_close)
        channel.add_on_return_callback(self._on_channel_return)
        # Need to confirm delivery so unroutable messages generate a return callback
        channel.confirm_delivery()
        declare_exchange(channel, self._exchange_name, self._on_exchange_declareok)

        # Declare the response queue
        channel.queue_declare(self._on_queue_declareok, exclusive=True, auto_delete=True)

    def _on_channel_close(self, channel, reply_code, reply_text):
        self._reset_channel()

    def _on_channel_return(self, channel, method, props, body):
        if props.correlation_id in self._responses:
            future = self._responses.pop(props.correlation_id)
            future.set_exception(RuntimeError("Message could not be delivered"))

    def _on_exchange_declareok(self, frame):
        self._exchange_bound = True
        self._start_publishing_if_ready()

    def _on_queue_declareok(self, frame):
        self._callback_queue_name = frame.method.queue
        self._channel.basic_consume(
            self._on_response, no_ack=True, queue=self._callback_queue_name)
        self._start_publishing_if_ready()

    def _start_publishing_if_ready(self):
        if self._callback_queue_name is not None and self._exchange_bound:
            self._send_queued_messages()

    def _send_msg(self, msg, future):
        correlation_id = str(uuid.uuid4())

        routing_key = "process.control.{}".format(msg['pid'])

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

        self._responses[correlation_id] = future
        return future

    def _on_response(self, ch, method, props, body):
        if props.correlation_id in self._responses:
            future = self._responses.pop(props.correlation_id)
            response = self._response_decode(body)
            future.set_result(response[_RESULT_KEY])

    def _send_queued_messages(self):
        for msg in self._queued_messages:
            self._send_msg(*msg)
        self._queued_messages = []


class RmqProcessController(pubsub.ConnectionListener):
    def __init__(self, process, connector,
                 exchange_name=defaults.CONTROL_EXCHANGE,
                 decoder=yaml.load,
                 response_encoder=yaml.dump):
        """
        Subscribes and listens for process control messages and acts on them
        by calling the corresponding methods of the process manager.

        :param connector: The RMQ connector
        :param exchange_name: The name of the exchange to use
        :param decoder:
        """
        self._process = process
        self._connector = connector
        self._exchange_name = exchange_name
        self._decode = decoder
        self._response_encode = response_encoder
        self._channel = None
        self._queue_name = None

        connector.add_connection_listener(self)
        if connector.is_connected:
            self._open_channel(connector.connection())

    def on_connection_opened(self, connector, connection):
        self._open_channel(connection)

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
        self._channel.queue_declare(self._on_queue_declareok, exclusive=True, auto_delete=True)

    def _on_queue_declareok(self, frame):
        """
        The queue as been declared, now bind it to the exchange using the
        routing keys we're listening for.
        """
        self._queue_name = frame.method.queue
        routing_key = "process.control.{}".format(self._process.pid)
        self._channel.queue_bind(
            self._on_bindok, self._queue_name, self._exchange_name,
            routing_key=routing_key)
        self._channel.queue_bind(
            None, self._queue_name, self._exchange_name,
            routing_key="process.control.[all]", nowait=True)

    def _on_bindok(self, unused_frame):
        """ The queue has been bound, we can start consuming. """
        self._channel.basic_consume(self._on_control, queue=self._queue_name)

    def _on_control(self, ch, method, props, body):
        d = self._decode(body)
        pid = d['pid']
        if pid != self._process.pid:
            return

        intent = d['intent']
        try:
            if intent == Action.PLAY:
                result = self._process.play()
            elif intent == Action.PAUSE:
                result = self._process.pause()
            elif intent == Action.CANCEL:
                result = self._process.cancel(msg=d.get('msg', None))
            else:
                raise RuntimeError("Unknown intent")
        except:
            result = 'EXCEPTION'

        # Tell the sender that we've dealt with it
        self._channel.basic_ack(method.delivery_tag)
        self._send_response(ch, props.reply_to, props.correlation_id, result)

    def _send_response(self, ch, reply_to, correlation_id, result):
        response = {_RESULT_KEY: result}
        add_host_info(response)
        ch.basic_publish(
            exchange='', routing_key=reply_to,
            properties=pika.BasicProperties(correlation_id=correlation_id),
            body=self._response_encode(response)
        )
