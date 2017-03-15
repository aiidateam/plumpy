import json
import pika
import time
import uuid
from plum.rmq.defaults import Defaults
from plum.rmq.util import Subscriber
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


class ProcessControlPublisher(object):
    """
    This class is responsible for sending control messages to processes e.g.
    play, pause, abort, etc.
    """

    def __init__(self, connection, queue=Defaults.TASK_CONTROL_QUEUE,
                 encoder=action_encode):
        self._queue = queue
        self._encode = encoder

        self._response = None
        self._correlation_id = None

        # Set up comms
        self._connection = connection
        self._channel = connection.channel()
        self._channel.confirm_delivery()
        self._channel.queue_declare(queue=self._queue)
        result = self._channel.queue_declare(exclusive=True)
        self._callback_queue = result.method.queue
        self._channel.basic_consume(self._on_response, no_ack=True, queue=self._callback_queue)

    def abort(self, pid, msg, timeout=None):
        return self._send_msg({'pid': pid, 'intent': Action.ABORT, 'msg': msg}, timeout)

    def pause(self, pid, timeout=None):
        return self._send_msg({'pid': pid, 'intent': Action.PAUSE}, timeout)

    def play(self, pid, timeout=None):
        return self._send_msg({'pid': pid, 'intent': Action.PLAY}, timeout)

    def _send_msg(self, msg, timeout=None):
        self._response = None
        self._correlation_id = str(uuid.uuid4())

        delivered = self._channel.basic_publish(
            exchange='', routing_key=self._queue,
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
            self._response = body


class ProcessControlSubscriber(Subscriber):
    def __init__(self, connection, queue=Defaults.TASK_CONTROL_QUEUE,
                 decoder=action_decode, process_manager=None):
        """
        Subscribes and listens for process control messages and acts on them
        by calling the corresponding methods of the process manager.

        :param connection: The RMQ connection object
        :param exchange: The name of the exchange to use
        :param decoder:
        :param process_manager: The process manager running the processes
            that are being controlled.
        :type process_manager: :class:`plum.process_manager.ProcessManager`
        """
        self._decode = decoder
        self._manager = process_manager
        self._stopping = False

        # Set up communications
        self._channel = connection.channel()
        self._channel.queue_declare(queue)
        self._channel.basic_consume(self._on_control, queue=queue)

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

    def set_process_manager(self, manager):
        self._manager = manager

    def _on_control(self, ch, method, props, body):
        if self._manager is None:
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
            return

        d = self._decode(body)
        pid = d['pid']
        intent = d['intent']
        result = 'OK'
        succeeded = True
        try:
            if intent == Action.PLAY:
                self._manager.play(pid)
            elif intent == Action.PAUSE:
                self._manager.pause(pid)
            elif intent == Action.ABORT:
                self._manager.abort(pid, d.get('msg', None), timeout=0)
            else:
                raise ValueError("Unknown intent")
        except BaseException as e:
            succeeded = False
            result = "{}: {}".format(e.__class__.__name__, e.message)

        if succeeded:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            # Tell the subscriber that we acted on the message
            ch.basic_publish(
                exchange='', routing_key=props.reply_to,
                properties=pika.BasicProperties(correlation_id=props.correlation_id),
                body=result
            )
        else:
            ch.basic_reject(delivery_tag=method.delivery_tag)

    def __del__(self):
        self._channel.close()
