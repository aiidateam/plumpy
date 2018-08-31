from functools import partial
import shutil
import tempfile
from tornado import testing
import unittest
import uuid

from kiwipy import rmq

import plumpy.test_utils

from plumpy import communications, process_comms, test_utils

try:
    import pika
except ImportError:
    pika = None

AWAIT_TIMEOUT = testing.get_async_test_timeout()


@unittest.skipIf(not pika, "Requires pika library and RabbitMQ")
class TestTaskActions(testing.AsyncTestCase):
    def setUp(self):
        super(TestTaskActions, self).setUp()
        self.loop = self.io_loop

        exchange_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())
        queue_name = "{}.{}.tasks".format(self.__class__.__name__, uuid.uuid4())

        self.rmq_communicator = rmq.connect(
            connection_params={'url': 'amqp://guest:guest@localhost:5672/'},
            message_exchange=exchange_name,
            task_queue=queue_name,
            testing_mode=True
        )
        self.communicator = communications.CommunicatorWrapper(self.rmq_communicator, self.loop)

        self._tmppath = tempfile.mkdtemp()
        self.persister = plumpy.PicklePersister(self._tmppath)
        # Add the process launcher
        self.communicator.add_task_subscriber(plumpy.ProcessLauncher(self.loop, persister=self.persister))

        self.process_controller = process_comms.RemoteProcessController(self.communicator)

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.rmq_communicator.stop()
        super(TestTaskActions, self).tearDown()
        shutil.rmtree(self._tmppath)

    @testing.gen_test
    def test_launch(self):
        # Let the process run to the end
        result = yield self.process_controller.launch_process(test_utils.DummyProcess)
        # Check that we got a result
        self.assertDictEqual(test_utils.DummyProcess.EXPECTED_OUTPUTS, result)

    @testing.gen_test
    def test_launch_nowait(self):
        """ Testing launching but don't wait, just get the pid """
        pid = yield self.process_controller.launch_process(test_utils.DummyProcess, nowait=True)
        self.assertIsInstance(pid, uuid.UUID)

    @testing.gen_test
    def test_execute_action(self):
        """ Test the process execute action """
        result = yield self.process_controller.execute_process(test_utils.DummyProcessWithOutput)
        self.assertEqual(test_utils.DummyProcessWithOutput.EXPECTED_OUTPUTS, result)

    @testing.gen_test
    def test_execute_action_nowait(self):
        """ Test the process execute action """
        pid = yield self.process_controller.execute_process(test_utils.DummyProcessWithOutput, nowait=True)
        self.assertIsInstance(pid, uuid.UUID)

    @testing.gen_test
    def test_launch_many(self):
        """Test launching multiple processes"""
        num_to_launch = 10

        launch_futures = []
        for _ in range(num_to_launch):
            task = partial(self.process_controller.launch_process, test_utils.DummyProcess, nowait=True)
            launch_futures.append(plumpy.create_task(task))

        results = yield launch_futures
        for result in results:
            self.assertIsInstance(result, uuid.UUID)

    @testing.gen_test
    def test_continue(self):
        """ Test continuing a saved process """
        process = test_utils.DummyProcessWithOutput()
        self.persister.save_checkpoint(process)
        pid = process.pid
        del process

        # Let the process run to the end
        result = yield self.process_controller.continue_process(pid)
        self.assertEqual(result, test_utils.DummyProcessWithOutput.EXPECTED_OUTPUTS)
