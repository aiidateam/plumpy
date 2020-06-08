# -*- coding: utf-8 -*-
import tempfile
import functools
import asyncio

from kiwipy import rmq
import plumpy


class DummyProcessWithOutput(plumpy.Process):
    EXPECTED_OUTPUTS = {'default': 5}

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.inputs.dynamic = True
        spec.outputs.dynamic = True
        spec.output('default', valid_type=int)

    def run(self):
        self.out('default', 5)


def main():
    with rmq.RmqThreadCommunicator.connect(connection_params={'url': 'amqp://127.0.0.1'}
                                           ) as comm, tempfile.TemporaryDirectory() as tmpdir:
        loop = asyncio.get_event_loop()
        persister = plumpy.PicklePersister(tmpdir)
        task_receiver = plumpy.ProcessLauncher(loop=loop, persister=persister)

        def callback(*args, **kwargs):
            fut = plumpy.create_task(functools.partial(task_receiver, *args, **kwargs), loop=loop)
            return fut

        comm.add_task_subscriber(callback)

        process_controller = plumpy.RemoteProcessThreadController(comm)

        future = process_controller.launch_process(DummyProcessWithOutput)

        async def task():
            result = await asyncio.wrap_future(future.result())
            print(result)

        loop.run_until_complete(task())


if __name__ == '__main__':
    main()
