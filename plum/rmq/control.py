import apricotpy
import json
from functools import partial
import pika
import pika.exceptions
import uuid

from plum.rmq.defaults import Defaults
from plum.rmq.util import add_host_info
from plum.utils import override

__all__ = ['ProcessControlPublisher', 'ProcessControlSubscriber']


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


_RESULT_KEY = 'result'
# This means that the intent has been actioned but not yet completed
_ACTION_SCHEDULED = 'SCHEDULED'
# This means that the intent has been completed
_ACTION_DONE = 'DONE'
# The action failed to be completed
_ACTION_FAILED = 'ACTION_FAILED'


class ProcessControlPublisher(apricotpy.TickingLoopObject):
    """
    This class is responsible for sending control messages to processes e.g.
    play, pause, abort, etc.
    """

    def __init__(self, connection, exchange=Defaults.CONTROL_EXCHANGE,
                 encoder=action_encode, response_decoder=json.loads):
        super(ProcessControlPublisher, self).__init__()

        self._exchange = exchange
        self._encode = encoder
        self._response_decode = response_decoder
        self._responses = {}

        # Set up comms
        self._connection = connection
        self._channel = connection.channel()
        self._channel.exchange_declare(exchange=exchange, exchange_type='fanout')
        # Response queue
        result = self._channel.queue_declare(exclusive=True)
        self._callback_queue = result.method.queue
        self._channel.basic_consume(self._on_response, no_ack=True, queue=self._callback_queue)

    def abort_process(self, pid, msg=None):
        return self._send_msg({'pid': pid, 'intent': Action.ABORT, 'msg': msg})

    def pause_process(self, pid):
        return self._send_msg({'pid': pid, 'intent': Action.PAUSE})

    def play_process(self, pid):
        return self._send_msg({'pid': pid, 'intent': Action.PLAY})

    @override
    def tick(self):
        self._channel.connection.process_data_events(time_limit=0.01)

    def _send_msg(self, msg):
        self._check_in_loop()

        correlation_id = str(uuid.uuid4())

        delivered = self._channel.basic_publish(
            exchange=self._exchange, routing_key='',
            properties=pika.BasicProperties(
                reply_to=self._callback_queue, correlation_id=correlation_id,
                delivery_mode=1, content_type='text/json'
            ),
            body=action_encode(msg),
        )

        if not delivered:
            return None

        future = self.loop().create_future()
        self._responses[correlation_id] = future

        return future

    def _on_response(self, ch, method, props, body):
        if props.correlation_id in self._responses:
            future = self._responses.pop(props.correlation_id)
            response = self._response_decode(body)
            result = response[_RESULT_KEY]
            if result == _ACTION_DONE:
                future.set_result(True)
            elif result == _ACTION_FAILED:
                future.set_result(False)
            elif result == _ACTION_SCHEDULED:
                # We have to wait until the action is completed, so give another future
                fut = self.loop().create_future()
                self._responses[props.correlation_id] = fut
                future.set_result(fut)
            else:
                raise ValueError("Unknown action result '{}'".format(result))

    def _check_in_loop(self):
        assert self.in_loop(), "Object is not in the event loop"


class ProcessControlSubscriber(apricotpy.TickingLoopObject):
    def __init__(self, connection, exchange=Defaults.CONTROL_EXCHANGE,
                 decoder=action_decode, response_encoder=json.dumps):
        """
        Subscribes and listens for process control messages and acts on them
        by calling the corresponding methods of the process manager.

        :param connection: The RMQ connection object
        :param exchange: The name of the exchange to use
        :param decoder:
        """
        super(ProcessControlSubscriber, self).__init__()

        self._decode = decoder
        self._response_encode = response_encoder
        self._stopping = False
        self._last_correlation_id = None

        # Set up communications
        self._channel = connection.channel()
        self._channel.exchange_declare(exchange, exchange_type='fanout')
        result = self._channel.queue_declare(exclusive=True)
        queue = result.method.queue
        self._channel.queue_bind(queue, exchange)
        self._channel.basic_consume(self._on_control, queue=queue, no_ack=True)

    @override
    def tick(self):
        self._channel.connection.process_data_events(time_limit=0.01)

    def _on_control(self, ch, method, props, body):
        d = self._decode(body)
        pid = d['pid']

        try:
            obj = self.loop().get_object(pid)
        except ValueError:
            # Not an object our loop knows about
            return

        intent = d['intent']
        try:
            if intent == Action.PLAY:
                obj.play()
                result = _ACTION_DONE
            elif intent == Action.PAUSE:
                obj.pause()
                result = _ACTION_DONE
            elif intent == Action.ABORT:
                fut = obj.abort(msg=d.get('msg', None))

                # When the abort is finished send another message to say it's done
                fut.add_done_callback(
                    partial(self._action_done, ch, props.reply_to, props.correlation_id)
                )

                result = _ACTION_SCHEDULED
            else:
                raise RuntimeError("Unknown intent")
        except ValueError as e:
            result = e.message
        else:
            # Tell the sender that we've dealt with it
            self._send_response(ch, props.reply_to, props.correlation_id, result)

    def _action_done(self, ch, reply_to, correlation_id, future):
        if future.result():
            result = _ACTION_DONE
        else:
            result = _ACTION_FAILED

        self._send_response(ch, reply_to, correlation_id, result)

    def _send_response(self, ch, reply_to, correlation_id, result):
        response = {_RESULT_KEY: result}
        add_host_info(response)
        ch.basic_publish(
            exchange='', routing_key=reply_to,
            properties=pika.BasicProperties(correlation_id=correlation_id),
            body=self._response_encode(response)
        )
