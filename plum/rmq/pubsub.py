from functools import partial
import pika
import pika.exceptions
import plum
import plum.utils
import logging
import traceback

__all__ = ['RmqConnector', 'ConnectionListener']

LOGGER = logging.getLogger(__name__)


class ConnectionListener(object):
    def on_connection_opened(self, connector, connection):
        pass

    def on_connection_closed(self, connector, reconnecting):
        pass


class RmqConnector(object):
    """
    An basic RMQ client that opens a connection and one channel.
    If an auto reconnect timeout is given it will try to keep the connection
    open by reopening if it is closed.
    """
    _connection = None

    def __init__(self, amqp_url,
                 auto_reconnect_timeout=None,
                 loop=None):
        self._url = amqp_url
        self._reconnect_timeout = auto_reconnect_timeout
        self._loop = loop
        self._channels = []

        self._event_helper = plum.utils.EventHelper(ConnectionListener)
        self._stopping = False

    @property
    def is_connected(self):
        return self._connection is not None and self._connection.is_open

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika. If you want the reconnection to work, make
        sure you set stop_ioloop_on_close to False, which is not the default
        behavior of this adapter.
        """
        LOGGER.info('Connecting to %s', self._url)
        pika.TornadoConnection(pika.URLParameters(self._url),
                               on_open_callback=self._on_connection_open,
                               on_close_callback=self._on_connection_closed,
                               stop_ioloop_on_close=False,
                               custom_ioloop=self._loop)

    def close(self):
        """Stop the example by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published. The IOLoop is started because this method is
        invoked by the Try/Catch below when KeyboardInterrupt is caught.
        Starting the IOLoop again will allow the publisher to cleanly
        disconnect from RabbitMQ.

        """
        LOGGER.info('Stopping')
        self._stopping = True
        self._close_channels()
        self._close_connection()

    def open_channel(self, callback):
        """This method will open a new channel with RabbitMQ by issuing the
        Channel.Open RPC command. When RabbitMQ confirms the channel is open
        by sending the Channel.OpenOK RPC reply, the callback method
        will be invoked.
        """
        assert self.connection() is not None, \
            "Can't open channel, not connected"
        if callback is None:
            raise ValueError("Must supply a callback")

        LOGGER.info('Creating a new channel')
        self._connection.channel(
            on_open_callback=partial(self._on_channel_open, callback))

    def connection(self):
        return self._connection

    def add_connection_listener(self, listener):
        self._event_helper.add_listener(listener)

    def remove_connection_listener(self, listener):
        self._event_helper.remove_listener(listener)

    def _on_connection_open(self, connection):
        """Called when the RMQ connection has been opened

        :type connection: pika.BaseConnection
        """
        LOGGER.info('Connection opened')
        self._connection = connection

        self._event_helper.fire_event(
            ConnectionListener.on_connection_opened, self, connection)

    def _on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channels = []

        reconnecting = False
        if not self._stopping and self._reconnect_timeout is not None:
            LOGGER.warning(
                "Connection closed, reopening in {} seconds: ({}) {}".format(
                    self._reconnect_timeout, reply_code, reply_text
                ))
            self._connection.add_timeout(self._reconnect_timeout, self._reconnect)
            reconnecting = True

        self._event_helper.fire_event(
            ConnectionListener.on_connection_closed,
            self, reconnecting)

    def _on_channel_open(self, client_callback, channel):
        try:
            client_callback(channel)
        except:
            LOGGER.warning(
                "Exception while calling client channel opened "
                "callback.  Closing channel.:\n{}".format(traceback.format_exc()))
            channel.close()
        else:
            self._channels.append(channel)
            channel.add_on_close_callback(self._on_channel_closed)

    def _on_channel_closed(self, channel, reply_code, reply_text):
        try:
            self._channels.remove(channel)
            LOGGER.info("Channel '{}' closed.  Code '{}', text '{}'".format(
                channel.channel_number, reply_code, reply_text))
        except ValueError:
            pass

    def _reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        if not self._stopping:
            # Create a new connection
            self.connect()

    def _close_channels(self):
        LOGGER.info('Closing channels')
        for ch in self._channels:
            try:
                ch.close()
            except pika.exceptions.ChannelAlreadyClosing:
                pass

    def _close_connection(self):
        """This method closes the connection to RabbitMQ."""
        if self._connection is not None:
            LOGGER.info('Closing connection')
            self._connection.close()


class _RmqConnection(object):
    """This is an example publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.

    """
    _connection = None
    _channel = None
    _starting_future = None

    def __init__(self, amqp_url,
                 queue_name,
                 queue_properties=None,
                 routing_key='',
                 exchange_name='',
                 exchange_type=None,
                 auto_reconnect_timeout=5.,
                 ioloop=None):
        self._url = amqp_url
        self._queue_name = queue_name
        self._queue_properties = queue_properties
        self._routing_key = routing_key
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._ioloop = ioloop

        self._stopping = False
        self._reconnect_timeout = auto_reconnect_timeout

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika. If you want the reconnection to work, make
        sure you set stop_ioloop_on_close to False, which is not the default
        behavior of this adapter.

        :rtype: :class:`plum.Future`

        """
        LOGGER.info('Connecting to %s', self._url)
        self._starting_future = plum.Future()
        pika.TornadoConnection(pika.URLParameters(self._url),
                               on_open_callback=self._on_connection_open,
                               on_close_callback=self._on_connection_closed,
                               stop_ioloop_on_close=False,
                               custom_ioloop=self._get_ioloop())

        return self._starting_future

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.
        """
        LOGGER.info('Declaring queue %s', queue_name)

        if self._queue_properties:
            kw = self._queue_properties
        else:
            kw = {}

        future = plum.Future()
        self._channel.queue_declare(
            partial(self._on_queue_declareok, future),
            queue_name,
            **kw)

        return future

    def close(self):
        """Stop the example by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published. The IOLoop is started because this method is
        invoked by the Try/Catch below when KeyboardInterrupt is caught.
        Starting the IOLoop again will allow the publisher to cleanly
        disconnect from RabbitMQ.

        """
        LOGGER.info('Stopping')
        self._stopping = True
        self._close_channel()
        self._close_connection()

    def _get_ioloop(self):
        if self._ioloop is None:
            return plum.get_event_loop()
        return self._ioloop

    def _on_connection_open(self, connection):
        """Called when the RMQ connection has been opened

        :type unused_connection: pika.BaseConnection
        """
        LOGGER.info('Connection opened')
        self._connection = connection
        self._open_channel()

    def _on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if not self._stopping and self._reconnect_timeout is not None:
            LOGGER.warning(
                "Connection closed, reopening in {} seconds: ({}) {}".format(
                    self._reconnect_timeout, reply_code, reply_text
                ))
            self._connection.add_timeout(self._reconnect_timeout, self._reconnect)

    def _reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        if not self._stopping:
            # Create a new connection
            self.connect()

    def _open_channel(self):
        """This method will open a new channel with RabbitMQ by issuing the
        Channel.Open RPC command. When RabbitMQ confirms the channel is open
        by sending the Channel.OpenOK RPC reply, the on_channel_open method
        will be invoked.

        """
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        """Called when the channel has been opened.

        :param pika.channel.Channel channel: The channel object
        """
        LOGGER.info('Channel opened')
        self._channel = channel

        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self._on_channel_closed)

        if self._exchange_name != '':
            self._setup_exchange(self._exchange_name)
        else:
            self._setup_queue(self._queue_name)

    def _on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        LOGGER.warning('Channel was closed: (%s) %s', reply_code, reply_text)
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def _setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOGGER.info('Declaring exchange %s', exchange_name)
        result = self._channel.exchange_declare(self._on_exchange_declareok,
                                                exchange_name,
                                                self._exchange_type)

        print(result)

    def _on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        LOGGER.info('Exchange declared')
        self.setup_queue(self._queue_name)

    def _on_queue_declareok(self, future, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame
        """
        LOGGER.info('Binding %s to %s with %s',
                    self._exchange_name, self._queue_name, self._routing_key)
        self._channel.queue_bind(
            partial(self._on_bindok, future),
            self._queue_name,
            self._exchange_name,
            self._routing_key
        )

    def _on_bindok(self, future, unused_frame):
        """This method is invoked by pika when it receives the Queue.BindOk
        response from RabbitMQ. Since we know we're now setup and bound, it's
        time to start publishing."""
        LOGGER.info('Queue bound')
        future.set_result(True)

    def _close_channel(self):
        """Invoke this command to close the channel with RabbitMQ by sending
        the Channel.Close RPC command.
        """
        if self._channel is not None:
            LOGGER.info('Closing the channel')
            self._channel.close()

    def _close_connection(self):
        """This method closes the connection to RabbitMQ."""
        if self._connection is not None:
            LOGGER.info('Closing connection')
            self._connection.close()


class BasicPublisher(_RmqConnection):
    """This is an example publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.

    """

    def __init__(self, amqp_url,
                 queue_name,
                 queue_properties=None,
                 routing_key='',
                 exchange_name='',
                 exchange_type=None,
                 auto_reconnect_timeout=5.,
                 ioloop=None):
        super(BasicPublisher, self).__init__(
            amqp_url=amqp_url,
            queue_name=queue_name,
            queue_properties=queue_properties,
            routing_key=routing_key,
            exchange_name=exchange_name,
            exchange_type=exchange_type,
            auto_reconnect_timeout=auto_reconnect_timeout,
            ioloop=ioloop)

        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None

    def publish_message(self, properties, message, routing_key=''):
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        on_delivery_confirmations method.

        Once the message has been sent, schedule another message to be sent.
        The main reason I put scheduling in was just so you can get a good idea
        of how the process is flowing by slowing down and speeding up the
        delivery intervals by changing the PUBLISH_INTERVAL constant in the
        class.

        """
        if self._channel is None or not self._channel.is_open:
            return

        routing_key = routing_key if routing_key else self._routing_key
        self._channel.basic_publish(self._exchange_name, routing_key,
                                    message, properties)
        self._message_number += 1
        message_future = plum.Future()
        self._deliveries[self._message_number] = message_future
        LOGGER.info('Published message # %i', self._message_number)

        return message_future

    def _on_bindok(self, unused_frame):
        """This method is invoked by pika when it receives the Queue.BindOk
        response from RabbitMQ. Since we know we're now setup and bound, it's
        time to start publishing."""
        super(BasicPublisher, self)._on_bindok(unused_frame)
        self._start_publishing()

    def _start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ

        """
        LOGGER.info('Issuing consumer related RPC commands')
        self._enable_delivery_confirmations()
        self._deliveries = {}
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        if self._starting_future is not None:
            self._starting_future.set_result(True)
            self._starting_future = None

    def _enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.

        """
        LOGGER.info('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self._on_delivery_confirmation)

    def _on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOGGER.info('Received %s for delivery tag: %i',
                    confirmation_type,
                    method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        future = self._deliveries.pop(method_frame.method.delivery_tag)
        LOGGER.info('Published %i messages, %i have yet to be confirmed, '
                    '%i were acked and %i were nacked',
                    self._message_number, len(self._deliveries),
                    self._acked, self._nacked)

        future.set_result(confirmation_type)


class BasicSubscriber(_RmqConnection):
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """
    _consumer_tag = None

    def __init__(self, amqp_url,
                 queue_name,
                 routing_key,
                 message_callback,
                 queue_properties=None,
                 exchange_name='',
                 exchange_type=None,
                 auto_reconnect_timeout=5.,
                 ioloop=None):
        super(BasicSubscriber, self).__init__(
            amqp_url=amqp_url,
            queue_name=queue_name,
            routing_key=routing_key,
            queue_properties=queue_properties,
            exchange_name=exchange_name,
            exchange_type=exchange_type,
            auto_reconnect_timeout=auto_reconnect_timeout,
            ioloop=ioloop
        )
        self._message_callback = message_callback

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        LOGGER.info('Stopping')
        self._stopping = True
        self._stop_consuming()
        LOGGER.info('Stopped')

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def _on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        super(BasicSubscriber, self)._on_bindok(unused_frame)
        self._start_consuming()

    def _start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        LOGGER.info('Issuing consumer related RPC commands')
        self._add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self._message_callback,
                                                         self._queue_name)

    def _add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by1 pika.

        """
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)

    def _on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        self.acknowledge_message(basic_deliver.delivery_tag)

    def _stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self._on_cancelok, self._consumer_tag)

    def _on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        self._close_channel()


class BasicPublisherSubscriber(BasicSubscriber, BasicPublisher):
    pass
