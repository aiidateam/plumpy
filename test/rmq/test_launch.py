import unittest
import uuid
import shutil
import tempfile

import plum.rmq
import plum.rmq.launch
import plum.test_utils
from plum import test_utils
from test.test_rmq import _HAS_PIKA
from test.util import TestCaseWithLoop

if _HAS_PIKA:
    import pika.exceptions
    from plum.rmq import ProcessLaunchPublisher, ProcessLaunchSubscriber


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestTaskControllerAndRunner(TestCaseWithLoop):
    def setUp(self):
        super(TestTaskControllerAndRunner, self).setUp()

        self.connector = plum.rmq.RmqConnector('amqp://guest:guest@localhost:5672/', loop=self.loop)
        self.exchange_name = "{}.{}".format(self.__class__.__name__, uuid.uuid4())
        self.queue_name = "{}.{}.tasks".format(self.__class__.__name__, uuid.uuid4())

        self.subscriber = ProcessLaunchSubscriber(
            self.connector, exchange_name=self.exchange_name, task_queue_name=self.queue_name, testing_mode=True)
        self.publisher = ProcessLaunchPublisher(
            self.connector, exchange_name=self.exchange_name, task_queue_name=self.queue_name, testing_mode=True)

        self.connector.connect()
        # Run the loop until until both are ready
        plum.run_until_complete(
            plum.gather(self.subscriber.initialised_future(), self.publisher.initialised_future()))

    def tearDown(self):
        # Close the connector before calling super because it will
        # close the loop
        self.connector.close()
        super(TestTaskControllerAndRunner, self).tearDown()

    def test_simple_launch(self):
        """Test simply launching a valid process"""
        launch_future = self.publisher.launch_process(test_utils.DummyProcessWithOutput)
        result = plum.run_until_complete(launch_future)
        self.assertIsNotNone(result)

    def test_simple_continue(self):
        # self.subscriber.close()
        tmppath = tempfile.mkdtemp()
        try:
            persister = plum.PicklePersister(tmppath)

            process = test_utils.DummyProcessWithOutput()
            persister.save_checkpoint(process)
            pid = process.pid
            del process

            subscriber = ProcessLaunchSubscriber(
                self.connector,
                exchange_name=self.exchange_name,
                task_queue_name=self.queue_name,
                testing_mode=True,
                persister=persister)

            future = self.publisher.continue_process(pid)
            self.assertTrue(plum.run_until_complete(future))
        finally:
            shutil.rmtree(tmppath)

    def test_launch_many(self):
        """Test launching multiple processes"""
        num_to_launch = 10

        launch_futures = []
        for _ in range(num_to_launch):
            future = self.publisher.launch_process(test_utils.DummyProcessWithOutput)
            launch_futures.append(future)

        results = plum.run_until_complete(plum.gather(*launch_futures))
        for result in results:
            self.assertIsInstance(result, uuid.UUID)


            # def test_launch(self):
            #     # Try launching a process
            #     launch = self._launch(plum.test_utils.DummyProcessWithOutput)
            #
            #     proc = None
            #     t0 = time.time()
            #     while proc is None and time.time() - t0 < 3.:
            #         self.runner_loop.tick()
            #         try:
            #             proc = self.runner_loop.get_process(launch.pid)
            #             break
            #         except ValueError:
            #             pass
            #     self.assertIsNotNone(proc)
            #
            #     result = proc.loop().run_until_complete(HansKlok(proc))
            #     awaitable_result = self.launcher_loop.run_until_complete(HansKlok(launch))
            #
            #     self.assertEqual(result, awaitable_result)
            #
            # def test_launch_cancel(self):
            #     # Try launching a process
            #     awaitable = self._launch(plum.test_utils.DummyProcessWithOutput)
            #
            #     proc = None
            #     t0 = time.time()
            #     while proc is None and time.time() - t0 < 3.:
            #         self.runner_loop.tick()
            #         try:
            #             proc = self.runner_loop.get_process(awaitable.pid)
            #             break
            #         except ValueError:
            #             pass
            #     self.assertIsNotNone(proc)
            #
            #     # Now cancel it
            #     proc.cancel()
            #     self.runner_loop.tick()
            #
            #     with self.assertRaises(apricotpy.CancelledError):
            #         self.launcher_loop.run_until_complete(HansKlok(awaitable))
            #
            # def test_launch_exception(self):
            #     # Try launching a process
            #     awaitable = self._launch(plum.test_utils.ExceptionProcess)
            #
            #     proc = None
            #     t0 = time.time()
            #     while proc is None and time.time() - t0 < 3.:
            #         self.runner_loop.tick()
            #         try:
            #             proc = self.runner_loop.get_process(awaitable.pid)
            #             break
            #         except ValueError:
            #             pass
            #     self.assertIsNotNone(proc)
            #
            #     # Now let it run
            #     with self.assertRaises(RuntimeError):
            #         result = proc.loop().run_until_complete(HansKlok(proc))
            #
            #     with self.assertRaises(RuntimeError):
            #         result = self.launcher_loop.run_until_complete(HansKlok(awaitable))
            #
            # def _launch(self, proc_class, *args, **kwargs):
            #     proc = self.launcher_loop.create(proc_class, *args, **kwargs)
            #     bundle = plum.Bundle(proc)
            #     return self.publisher.launch(bundle)
