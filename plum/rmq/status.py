import collections
import json
import pika
import uuid

from plum import Process
from plum.loop.objects import LoopObject, Ticking
from plum.rmq.defaults import Defaults
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


RequestInfo = collections.namedtuple('RequestInfo', ['future', 'responses', 'callback'])


class ProcessStatusRequester(Ticking, LoopObject):
    """
    This class can be used to request the status of processes
    """

    def __init__(self, loop, connection,
                 exchange=Defaults.STATUS_REQUEST_EXCHANGE, decoder=status_decode):
        super(ProcessStatusRequester, self).__init__(loop)

        self._exchange = exchange
        self._decode = decoder
        self._requests = {}

        # Set up communications
        self._channel = connection.channel()
        result = self._channel.queue_declare(exclusive=True)
        self._callback_queue = result.method.queue
        self._channel.exchange_declare(exchange=self._exchange, type='fanout')
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


class ProcessStatusSubscriber(Ticking, LoopObject):
    """
    This class listens for messages asking for a status update from a group of 
    processes.
    """

    def __init__(self, loop, connection,
                 exchange=Defaults.STATUS_REQUEST_EXCHANGE,
                 decoder=status_request_decode, encoder=status_encode):
        super(ProcessStatusSubscriber, self).__init__(loop)

        self._decode = decoder
        self._encode = encoder
        self._stopping = False

        # Set up communications
        self._channel = connection.channel()
        self._channel.exchange_declare(exchange=exchange, type='fanout')
        result = self._channel.queue_declare(exclusive=True)
        self._channel.queue_bind(exchange=exchange, queue=result.method.queue)
        self._channel.basic_consume(self._on_request, queue=result.method.queue)

    @override
    def tick(self):
        self._channel.connection.process_data_events(time_limit=0.1)

    def _on_request(self, ch, method, props, body):
        # d = self._decode(body)

        proc_status = {}
        for p in self.loop().objects(obj_type=Process):
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
            'waiting_on': str(process.get_waiting_on())
        }
