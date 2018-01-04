import collections
import json
import pickle
import pika
import uuid

import plum
from plum.rmq.defaults import Defaults
from . import utils
from plum.utils import override

__all__ = ['ProcessStatusRequester', 'ProcessStatusSubscriber']

PROCS_KEY = 'procs'


def status_request_decode(msg):
    d = json.loads(msg)
    try:
        d['pid'] = uuid.UUID(d['pid'])
    except ValueError:
        pass
    return d


RequestInfo = collections.namedtuple('RequestInfo', ['future', 'responses', 'callback'])


class ProcessStatusRequester(object):
    """
    This class can be used to request the status of processes
    """

    def __init__(self, connection, exchange=Defaults.STATUS_REQUEST_EXCHANGE,
                 decoder=pickle.loads, loop=None):
        self._exchange = exchange
        self._decode = decoder
        self._requests = {}

        # Set up communications
        self._channel = connection.channel()
        result = self._channel.queue_declare(exclusive=True)
        self._callback_queue = result.method.queue
        self._channel.exchange_declare(exchange=self._exchange, exchange_type='fanout')
        self._channel.basic_consume(self._on_response, no_ack=True, queue=self._callback_queue)

    def send_request(self, callback=None, timeout=1.0):
        if self.loop() is None:
            return None

        correlation_id = str(uuid.uuid4())
        self._channel.basic_publish(
            exchange=self._exchange, routing_key='',
            properties=pika.BasicProperties(
                reply_to=self._callback_queue,
                correlation_id=correlation_id
            ),
            body=""
        )

        future = self.loop().create_future()
        self._requests[correlation_id] = RequestInfo(future=future, responses=[], callback=callback)

        if timeout is not None:
            self.loop().call_later(timeout, self._on_response_deadline, correlation_id)

        return future

    @override
    def tick(self):
        self._channel.connection.process_data_events(time_limit=0.1)

    def _on_response(self, ch, method, props, body):
        try:
            rinfo = self._requests[props.correlation_id]

            response = self._decode(body)
            if rinfo.callback is not None:
                rinfo.callback(response)

            # WARNING: We save the responses, this could grow indefinitely if there is not deadline
            rinfo.responses.append(response)

        except KeyError:
            pass

    def _on_response_deadline(self, correlation_id):
        rinfo = self._requests.pop(correlation_id)
        rinfo.future.set_result(rinfo.responses)


class ProcessStatusSubscriber(object):
    """
    This class listens for messages asking for a status update from a group of 
    processes.
    """

    def __init__(self, connection,
                 exchange=Defaults.STATUS_REQUEST_EXCHANGE,
                 decoder=status_request_decode, encoder=pickle.dumps, loop=None):
        self._decode = decoder
        self._encode = encoder
        self._stopping = False
        self._last_props = None

        # Set up communications
        self._channel = connection.channel()
        self._channel.exchange_declare(exchange=exchange, exchange_type='fanout')
        result = self._channel.queue_declare(exclusive=True)
        self._channel.queue_bind(exchange=exchange, queue=result.method.queue)
        self._channel.basic_consume(self._on_request, queue=result.method.queue)

    @override
    def tick(self):
        self._channel.connection.process_data_events(time_limit=0.1)

    def _on_request(self, ch, method, props, body):
        # d = self._decode(body)

        # Send message to all Proecsses asking for a status update
        self.send_message(subject=plum.ProcessAction.REPORT_STATUS)
        self._last_props = props
        # Always acknowledge
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def message_received(self, subject, body, sender_id):
        if subject == plum.ProcessMessage.STATUS_REPORT:
            response = dict(body)
            utils.add_host_info(response)
            self._channel.basic_publish(
                exchange='', routing_key=self._last_props.reply_to,
                properties=pika.BasicProperties(correlation_id=self._last_props.correlation_id),
                body=self._encode(response)
            )
