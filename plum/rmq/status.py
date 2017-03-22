import json
import time
import uuid

import pika

from plum.rmq.defaults import Defaults
from plum.rmq.util import Subscriber
from plum.rmq.util import add_host_info
from plum.util import override

PROCS_KEY = 'procs'


def status_decode(msg):
    decoded = json.loads(msg)
    procs = decoded[PROCS_KEY]
    for pid in procs.keys():
        try:
            new_pid = uuid.UUID(pid)
            procs[new_pid] = procs.pop(pid)
        except ValueError:
            pass
    return decoded


def status_encode(response_):
    response = response_.copy()
    procs = response[PROCS_KEY]
    # UUID pids get converted to strings
    for pid in procs.keys():
        procs[pid]['state'] = procs[pid]['state'].name

        if isinstance(pid, uuid.UUID):
            procs[str(pid)] = procs.pop(pid)
    return json.dumps(response)


def status_request_decode(msg):
    d = json.loads(msg)
    try:
        d['pid'] = uuid.UUID(d['pid'])
    except ValueError:
        pass
    return d


class ProcessStatusRequester(object):
    """
    This class can be used to request the status of processes
    """

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
        self._responses = []
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
        deadline = time.time() + timeout if timeout is not None else None

        self.send_request()
        # Make sure to poll at least once
        self.poll_response(callback, 0)

        while deadline is None or time.time() < deadline:
            self.poll_response(callback, 0)

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

            self._responses.append(response)


class ProcessStatusSubscriber(Subscriber):
    """
    This class listens for messages asking for a status update on the processes
    currently being managed by a process manager and responds accordingly.
    """

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

    @override
    def start(self, poll_time=1.0):
        while self._channel._consumer_infos:
            self.poll(poll_time)
            if self._stopping:
                self._channel.stop_consuming()
                self._stopping = False

    @override
    def poll(self, time_limit=1.0):
        self._channel.connection.process_data_events(time_limit=time_limit)

    @override
    def stop(self):
        self._stopping = True

    @override
    def shutdown(self):
        self._channel.close()

    def _on_request(self, ch, method, props, body):
        # d = self._decode(body)

        proc_status = {}
        for p in self._manager.get_processes():
            proc_status[p.pid] = self._get_status(p)

        response = {PROCS_KEY: proc_status}
        add_host_info(response)

        if response:
            ch.basic_publish(
                exchange='', routing_key=props.reply_to,
                properties=pika.BasicProperties(correlation_id=props.correlation_id),
                body=self._encode(response)
            )
        # Always acknowledge
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _get_status(self, process):
        """
        Generate the status dictionary

        :param process: The process to generate the dictionary for
        :type process: :class:`plum.process.Process`
        :return: The status dictionary
        :rtype: dict
        """
        return {
            'creation_time': process.creation_time,
            'state': process.state,
            'playing': process.is_playing(),
            'waiting_on': str(process.get_waiting_on())
        }


