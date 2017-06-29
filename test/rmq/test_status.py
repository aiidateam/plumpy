import unittest
from test.util import TestCase

try:
    import pika
    import pika.exceptions
    import plum.rmq.status as status
    from plum.rmq.status import ProcessStatusSubscriber, ProcessStatusRequester, status_decode

    _HAS_PIKA = True
except ImportError:
    _HAS_PIKA = False
import uuid

from plum.loop.event_loop import BaseEventLoop
from plum.test_utils import WaitForSignalProcess
from plum.process import ProcessState
from plum.wait_ons import run_until


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestStatusRequesterAndProvider(TestCase):
    def setUp(self):
        super(TestStatusRequesterAndProvider, self).setUp()

        self.response = None

        # Set up communications
        try:
            self._connection = pika.BlockingConnection()
        except pika.exceptions.ConnectionClosed:
            self.fail("Couldn't open connection.  Make sure rmq server is running")

        exchange = "{}.{}.status_request".format(self.__class__.__name__, uuid.uuid4())
        self.requester = ProcessStatusRequester(self._connection, exchange=exchange)
        self.subscriber = ProcessStatusSubscriber(self._connection, exchange=exchange)

        self.loop = BaseEventLoop()
        self.loop.insert(self.requester)
        self.loop.insert(self.subscriber)

    def tearDown(self):
        super(TestStatusRequesterAndProvider, self).tearDown()
        self._connection.close()

    def test_request(self):
        procs = []
        for i in range(10):
            procs.append(WaitForSignalProcess.new())
            self.loop.insert(procs[-1])
        run_until(procs, ProcessState.WAITING, self.loop)

        future = self.requester.send_request(timeout=0.1)
        self.loop.run_until_complete(future)

        responses = future.result()
        self.assertEqual(len(responses), 1)
        procs_info = responses[0][status.PROCS_KEY]
        self.assertEqual(len(procs_info), len(procs))
        self.assertSetEqual(set(procs_info.keys()), {p.pid for p in procs})


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestStatusProvider(TestCase):
    def setUp(self):
        super(TestStatusProvider, self).setUp()
        self._response = None
        self._corr_id = None

        try:
            self._connection = pika.BlockingConnection()
        except pika.exceptions.ConnectionClosed:
            self.fail("Couldn't open connection.  Make sure _rmq server is running")

        self.channel = self._connection.channel()

        # Set up the request exchange
        self.request_exchange = '{}.{}.task_control'.format(self.__class__, uuid.uuid4())
        self.channel.exchange_declare(exchange=self.request_exchange, type='fanout')

        # Set up the response queue
        result = self.channel.queue_declare(exclusive=True)
        self.response_queue = result.method.queue
        self.channel.basic_consume(self._on_response, no_ack=True, queue=self.response_queue)

        self.subscriber = ProcessStatusSubscriber(self._connection, exchange=self.request_exchange)

        self.loop = BaseEventLoop()
        self.loop.insert(self.subscriber)

    def tearDown(self):
        super(TestStatusProvider, self).tearDown()
        self.channel.close()
        self._connection.close()

    def test_no_processes(self):
        response = status_decode(self._send_and_get())
        self.assertEqual(len(response[status.PROCS_KEY]), 0)

    def test_status(self):
        procs = []
        for i in range(20):
            procs.append(WaitForSignalProcess.new())
            self.loop.insert(procs[-1])
        run_until(procs, ProcessState.WAITING, self.loop)

        procs_dict = status_decode(self._send_and_get())[status.PROCS_KEY]
        self.assertEqual(len(procs_dict), len(procs))
        self.assertSetEqual(
            set([p.pid for p in procs]),
            set(procs_dict.keys())
        )

        playing = set([entry['playing'] for entry in procs_dict.itervalues()])
        self.assertSetEqual(playing, {True})

        for proc in procs:
            proc.abort()
        run_until(procs, ProcessState.STOPPED, self.loop)

        response = status_decode(self._send_and_get())
        self.assertEqual(len(response[status.PROCS_KEY]), 0)

    def _send_and_get(self):
        self._send_request()
        self._get_response()
        return self._response

    def _send_request(self):
        self._response = None
        self._corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange=self.request_exchange, routing_key='',
            properties=pika.BasicProperties(reply_to=self.response_queue,
                                            correlation_id=self._corr_id),
            body=""
        )

    def _get_response(self):
        self.loop.tick()
        self.channel.connection.process_data_events(time_limit=0.1)

    def _on_response(self, ch, method, props, body):
        if self._corr_id == props.correlation_id:
            self._response = body
