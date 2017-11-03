import apricotpy
import time
import unittest
import uuid

import plum.rmq.launch
import plum.test_utils
from test.test_rmq import _HAS_PIKA
from test.util import TestCase

if _HAS_PIKA:
    import pika.exceptions
    from plum.rmq import ProcessLaunchPublisher, ProcessLaunchSubscriber


def _create_temporary_queue(connection):
    channel = connection.channel()
    result = channel.queue_declare(exclusive=True)
    return result.method.queue


@unittest.skipIf(not _HAS_PIKA, "Requires pika library and RabbitMQ")
class TestTaskControllerAndRunner(TestCase):
    def setUp(self):
        super(TestTaskControllerAndRunner, self).setUp()

        try:
            self._connection = pika.BlockingConnection()
        except pika.exceptions.ConnectionClosed:
            self.fail("Couldn't open connection.  Make sure rmq server is running")

        self.launcher_loop = plum.new_event_loop()
        self.runner_loop = plum.new_event_loop()

        queue = "{}.{}.tasks".format(self.__class__.__name__, uuid.uuid4())

        self.publisher = self.launcher_loop.create(
            ProcessLaunchPublisher, self._connection, queue=queue)
        self.subscriber = self.runner_loop.create(
            ProcessLaunchSubscriber, self._connection, queue=queue)

    def tearDown(self):
        self._connection.close()
        self.launcher_loop.close()
        self.launcher_loop = None

    def test_launch(self):
        # Try launching a process
        awaitable = self._launch(plum.test_utils.DummyProcessWithOutput)

        proc = None
        t0 = time.time()
        while proc is None and time.time() - t0 < 3.:
            self.runner_loop.tick()
            proc = self.runner_loop.get_process(awaitable.pid)
            procs = self.runner_loop.objects(obj_type=plum.test_utils.DummyProcessWithOutput)
            if len(procs) > 0 and procs[0].pid == awaitable.pid:
                proc = procs[0]
                break
        self.assertIsNotNone(proc)

        result = ~proc
        awaitable_result = self.launcher_loop.run_until_complete(awaitable)

        self.assertEqual(result, awaitable_result)

    def test_launch_cancel(self):
        # Try launching a process
        awaitable = self._launch(plum.test_utils.DummyProcessWithOutput)

        proc = None
        t0 = time.time()
        while proc is None and time.time() - t0 < 3.:
            self.runner_loop.tick()
            procs = self.runner_loop.objects(obj_type=plum.test_utils.DummyProcessWithOutput)
            if len(procs) > 0 and procs[0].pid == awaitable.pid:
                proc = procs[0]
                break
        self.assertIsNotNone(proc)

        # Now cancel it
        proc.cancel()
        self.runner_loop.tick()

        with self.assertRaises(apricotpy.CancelledError):
            self.launcher_loop.run_until_complete(awaitable)

    def test_launch_exception(self):
        # Try launching a process
        awaitable = self._launch(plum.test_utils.ExceptionProcess)

        proc = None
        t0 = time.time()
        while proc is None and time.time() - t0 < 3.:
            self.runner_loop.tick()
            procs = self.runner_loop.objects(obj_type=plum.test_utils.ExceptionProcess)
            if len(procs) > 0 and procs[0].pid == awaitable.pid:
                proc = procs[0]
                break
        self.assertIsNotNone(proc)

        # Now let it run
        with self.assertRaises(RuntimeError):
            result = ~proc

        with self.assertRaises(RuntimeError):
            result = self.launcher_loop.run_until_complete(awaitable)

    def _launch(self, proc_class, *args, **kwargs):
        proc = self.launcher_loop.create(proc_class, *args, **kwargs)
        bundle = plum.Bundle(proc)
        return self.publisher.launch(bundle)
