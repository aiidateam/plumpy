import json
import pika
import pika.exceptions
import time
import uuid
from plum.rmq.defaults import Defaults
from plum.rmq.util import Subscriber, add_host_info
from plum.util import override


class Action(object):
    PLAY = 'play'
    PAUSE = 'pause'
    ABORT = 'abort'


def action_decode(msg):
    d = json.loads(msg)
    if isinstance(d['pid'], basestring):
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


RESULT_KEY = 'result'


class ProcessControlPublisher(object):
    """
    This class is responsible for sending control messages to processes e.g.
    play, pause, abort, etc.
    """

    def __init__(self, connection, exchange=Defaults.CONTROL_EXCHANGE,
                 encoder=action_encode, response_decoder=json.loads):
        self._exchange = exchange
        self._encode = encoder
        self._response_decode = response_decoder

        self._response = None
        self._correlation_id = None

        # Set up comms
        self._connection = connection
        self._channel = connection.channel()
        self._channel.exchange_declare(exchange=exchange, type='fanout')
        result = self._channel.queue_declare(exclusive=True)
        self._callback_queue = result.method.queue
        self._channel.basic_consume(self._on_response, no_ack=True, queue=self._callback_queue)

    def abort(self, pid, msg=None, timeout=None):
        return self._send_msg({'pid': pid, 'intent': Action.ABORT, 'msg': msg}, timeout)

    def pause(self, pid, timeout=None):
        return self._send_msg({'pid': pid, 'intent': Action.PAUSE}, timeout)

    def play(self, pid, timeout=None):
        return self._send_msg({'pid': pid, 'intent': Action.PLAY}, timeout)

    def _send_msg(self, msg, timeout=None):
        self._response = None
        self._correlation_id = str(uuid.uuid4())

        delivered = self._channel.basic_publish(
            exchange=self._exchange, routing_key='',
            properties=pika.BasicProperties(
                reply_to=self._callback_queue, correlation_id=self._correlation_id,
                delivery_mode=1, content_type='text/json'
            ),
            body=action_encode(msg),
        )

        if not delivered:
            return False

        deadline = time.time() + timeout if timeout is not None else None
        while self._response is None:
            self._connection.process_data_events()
            if deadline is not None and time.time() >= deadline:
                break

        return self._response

    def _on_response(self, ch, method, props, body):
        if self._correlation_id == props.correlation_id:
            self._response = self._response_decode(body)

    def __del__(self):
        self._channel.close()


class ProcessControlSubscriber(Subscriber):
    def __init__(self, connection, exchange=Defaults.CONTROL_EXCHANGE,
                 decoder=action_decode, process_controller=None, response_encoder=json.dumps):
        """
        Subscribes and listens for process control messages and acts on them
        by calling the corresponding methods of the process manager.

        :param connection: The RMQ connection object
        :param exchange: The name of the exchange to use
        :param decoder:
        :param process_controller: The process controller running the processes
            that are being controlled.
        :type process_controller: :class:`plum.process_controller.ProcessController`
        """
        self._decode = decoder
        self._controller = process_controller
        self._response_encode = response_encoder
        self._stopping = False
        self._last_correlation_id = None

        # Set up communications
        self._channel = connection.channel()
        self._channel.exchange_declare(exchange, type='fanout')
        result = self._channel.queue_declare(exclusive=True)
        queue = result.method.queue
        self._channel.queue_bind(queue, exchange)
        self._channel.basic_consume(self._on_control, queue=queue, no_ack=True)

    @override
    def start(self, poll_time=0.2):
        while self._channel._consumer_infos:
            self.poll(poll_time)
            if self._stopping:
                self._channel.stop_consuming()
                self._stopping = False

    @override
    def poll(self, time_limit=0.2):
        self._channel.connection.process_data_events(time_limit=time_limit)

    @override
    def stop(self):
        self._stopping = True

    def set_process_controller(self, controller):
        self._controller = controller

    def _on_control(self, ch, method, props, body):
        d = self._decode(body)
        pid = d['pid']
        intent = d['intent']
        try:
            if intent == Action.PLAY:
                self._controller.play(pid)
                result = 'PLAYED'
            elif intent == Action.PAUSE:
                self._controller.pause(pid, timeout=0.)
                result = 'PAUSING'
            elif intent == Action.ABORT:
                self._controller.abort(pid, d.get('msg', None), timeout=0.)
                result = 'ABORTING'
            else:
                raise RuntimeError("Unknown intent")
        except BaseException:
            pass
        else:
            response = {RESULT_KEY: result}
            add_host_info(response)

            # Tell the sender that we've dealt with it
            ch.basic_publish(
                exchange='', routing_key=props.reply_to,
                properties=pika.BasicProperties(correlation_id=props.correlation_id),
                body=self._response_encode(response)
            )

    def shutdown(self):
        self._channel.close()

    def __del__(self):
        try:
            self.shutdown()
        except pika.exceptions.ChannelClosed:
            pass
