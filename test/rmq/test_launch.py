import apricotpy
import time
import unittest
import uuid

import plum.rmq.launch
import plum.test_utils
from plum import test_utils
from test.test_rmq import _HAS_PIKA
from test.util import TestCase

if _HAS_PIKA:
    import pika.exceptions
    from plum.rmq import ProcessLaunchPublisher, ProcessLaunchSubscriber

AMQP_URL = 'amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600'


# @unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
@unittest.skip("Refactoring RMQ support")
class TestTaskControllerAndRunner(TestCase):
    def setUp(self):
        super(TestTaskControllerAndRunner, self).setUp()

        queue_name = "{}.{}.tasks".format(self.__class__.__name__, uuid.uuid4())

        self.publisher = ProcessLaunchPublisher(AMQP_URL, queue_name)
        self.subscriber = ProcessLaunchSubscriber(AMQP_URL, queue_name)

        self._run_until_complete(
            plum.gather(self.publisher.connect(), self.subscriber.connect()))

    def tearDown(self):
        self._run_until_complete(
            plum.gather(self.publisher.close(), self.subscriber.close()))

    def test_simple_launch(self):
        """Test simply launching a valid process"""
        launch_future = self.publisher.launch(test_utils.DummyProcessWithOutput)
        result = self._run_until_complete(launch_future)

        self.assertTrue(result)

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

    def _run_until_complete(self, future, loop=None):
        if loop is None:
            loop = plum.get_event_loop()

        def stop(fut):
            loop.stop()

        future.add_done_callback(stop)
        loop.start()
        return future.result()
