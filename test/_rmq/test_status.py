

from test.util import TestCase
import pika
import pika.exceptions
import json
import uuid
from plum._rmq.status import StatusProvider, StatusRequester, status_decode
from plum.process_manager import ProcessManager
from plum.test_utils import WaitForSignalProcess
from plum.process import ProcessState
from plum.wait_ons import wait_until


class TestStatusRequesterAndProvider(TestCase):
    def setUp(self):
        super(TestStatusRequesterAndProvider, self).setUp()

        self.response = None

        # Set up communications
        try:
            self._connection = pika.BlockingConnection()
        except pika.exceptions.ConnectionClosed:
            self.fail("Couldn't open connection.  Make sure rmq server is running")

        exchange = "{}.{}.status_request".format(self.__class__, uuid.uuid4())
        self.requester = StatusRequester(self._connection, exchange=exchange)
        self.manager = ProcessManager()
        self.provider = StatusProvider(
            self._connection, process_manager=self.manager, exchange=exchange)

    def tearDown(self):
        super(TestStatusRequesterAndProvider, self).tearDown()
        self._connection.close()

    def test_status_decode(self):
        pid = uuid.uuid4()
        status = {
            str(pid): {
                'state': str(ProcessState.WAITING),
                'playing': True
            }
        }
        decoded = status_decode(json.dumps(status))
        self.assertEqual(len(decoded), 1)
        self.assertIn(pid, decoded)
        self.assertEqual(decoded[pid]['state'], str(ProcessState.WAITING))
        self.assertEqual(decoded[pid]['playing'], True)

    def test_request(self):
        procs = []
        for i in range(0, 10):
            procs.append(WaitForSignalProcess.new_instance())
            self.manager.start(procs[-1])

        response = self._send_request_poll_response(0.2)
        self.assertEqual(len(response), len(procs))
        self.assertSetEqual(set(response.keys()), {p.pid for p in procs})

        self.assertTrue(
            self.manager.abort_all(timeout=10),
            "Failed to abort processes within timeout"
        )

        response = self._send_request_poll_response(0.2)
        self.assertIsNone(response)

    def _send_request_poll_response(self, timeout):
        self.requester.send_request()
        self.provider.poll(timeout)
        return self.requester.poll_response(timeout=timeout)


class TestStatusProvider(TestCase):
    def setUp(self):
        super(TestStatusProvider, self).setUp()
        self._response = None
        self._corr_id = None

        try:
            self._connection = pika.BlockingConnection()
        except pika.exceptions.ConnectionClosed:
            self.fail("Couldn't open connection.  Make sure rmq server is running")

        self.channel = self._connection.channel()

        # Set up the request exchange
        self.request_exchange = '{}.{}.task_control'.format(
            self.__class__, uuid.uuid4())
        self.channel.exchange_declare(exchange=self.request_exchange, type='fanout')

        # Set up the response queue
        result = self.channel.queue_declare(exclusive=True)
        self.response_queue = result.method.queue
        self.channel.basic_consume(
            self._on_response, no_ack=True, queue=self.response_queue)

        self.manager = ProcessManager()
        self.provider = StatusProvider(
            self._connection, exchange=self.request_exchange,
            process_manager=self.manager)

    def tearDown(self):
        super(TestStatusProvider, self).tearDown()
        self.channel.close()
        self._connection.close()

    def test_no_processes(self):
        response = self._send_and_get()
        self.assertIsNone(response)

    def test_status(self):
        procs = []
        for i in range(0, 20):
            procs.append(WaitForSignalProcess.new_instance())
        for p in procs:
            self.manager.start(p)
        wait_until(procs, ProcessState.WAITING)

        response = self._send_and_get()
        d = json.loads(response)
        self.assertEqual(len(d), len(procs))
        self.assertSetEqual(
            set([str(p.pid) for p in procs]),
            set(d.keys())
        )

        playing = set([entry['playing'] for entry in d.itervalues()])
        self.assertSetEqual(playing, {True})

        self.assertTrue(
            self.manager.abort_all(timeout=10),
            "Couldn't abort all processes in timeout")

        response = self._send_and_get()
        self.assertIsNone(response)

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
        self.provider.poll(1)
        self.channel.connection.process_data_events(time_limit=0.1)

    def _on_response(self, ch, method, props, body):
        if self._corr_id == props.correlation_id:
            self._response = body