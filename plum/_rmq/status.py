import json
import uuid

import pika

from plum._rmq import Defaults, Subscriber


def status_decode(msg):
    raw = json.loads(msg)
    decoded = {}
    for pid_, info in raw.iteritems():
        try:
            pid = uuid.UUID(pid_)
        except ValueError:
            pid = pid_
        decoded[pid] = info
    return decoded


def status_encode(response):
    d = {}
    for pid, info in response.iteritems():
        if isinstance(pid, uuid.UUID):
            pid = str(pid)
        d[pid] = info
        d[pid]['state'] = str(d[pid]['state'])
    return json.dumps(d)


def status_request_decode(msg):
    d = json.loads(msg)
    try:
        d['pid'] = uuid.UUID(d['pid'])
    except ValueError:
        pass
    return d


class StatusRequester(object):
    def __init__(self, connection, exchange=Defaults.STATUS_REQUEST_EXCHANGE,
                 decoder=status_decode):
        self._exchange = exchange
        self._decode = decoder
        self._corr_id = None
        self._callback = None
        self._responses = None

        # Set up communications
        self._channel = connection.channel()
        result = self._channel.queue_declare(exclusive=True)
        self._callback_queue = result.method.queue
        self._channel.exchange_declare(exchange=self._exchange, type='fanout')
        self._channel.basic_consume(self._on_response, no_ack=True,
                                    queue=self._callback_queue)

    def send_request(self):
        self._responses = None
        self._corr_id = str(uuid.uuid4())
        self._channel.basic_publish(
            exchange=self._exchange, routing_key='',
            properties=pika.BasicProperties(
                reply_to=self._callback_queue,
                correlation_id=self._corr_id
            ),
            body=""
        )

    def request(self, callback=None, timeout=1):
        self.send_request()
        self.poll_response(callback, timeout)
        return self._responses

    def poll_response(self, callback=None, timeout=1):
        if self._corr_id is None:
            return None

        self._callback = callback
        self._channel.connection.process_data_events(time_limit=timeout)
        self._callback = None
        return self._responses

    def _on_response(self, ch, method, props, body):
        if self._corr_id == props.correlation_id:
            response = self._decode(body)
            if self._callback is not None:
                self._callback(response)
            try:
                self._responses.update(response)
            except AttributeError:
                self._responses = response


class StatusProvider(Subscriber):
    def __init__(self, connection, process_manager=None,
                 exchange=Defaults.STATUS_REQUEST_EXCHANGE,
                 decoder=status_request_decode, encoder=status_encode):
        self._manager = process_manager
        self._decode = decoder
        self._encode = encoder
        self._stopping = False

        # Set up communications
        self._channel = connection.channel()
        self._channel.exchange_declare(exchange=exchange, type='fanout')
        result = self._channel.queue_declare(exclusive=True)
        self._channel.queue_bind(exchange=exchange, queue=result.method.queue)
        self._channel.basic_consume(self._on_request, queue=result.method.queue)

    def set_process_manager(self, manager):
        self._manager = manager

    def start(self):
        while self._channel._consumer_infos:
            self.poll()
            if self._stopping:
                self._channel.stop_consuming()
                self._stopping = False

    def poll(self, time_limit=1):
        self._channel.connection.process_data_events(time_limit=time_limit)

    def stop(self):
        self._stopping = True

    def _on_request(self, ch, method, props, body):
        # d = self._decode(body)

        response = {}
        for p in self._manager.get_processes():
            response[p.pid] = self._get_status(p)

        if response:
            ch.basic_publish(
                exchange='', routing_key=props.reply_to,
                properties=pika.BasicProperties(correlation_id=props.correlation_id),
                body=self._encode(response)
            )
        # Always acknowledge
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _get_status(self, process):
        return {
            'state': process.state,
            'playing': process.is_executing()
        }
