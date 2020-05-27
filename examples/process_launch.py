# -*- coding: utf-8 -*-
import tempfile
import functools
import asyncio

import kiwipy
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
    with kiwipy.rmq.connect('amqp://127.0.0.1') as comm:
        loop = asyncio.get_event_loop()
        persister = plumpy.PicklePersister(tempfile.mkdtemp())
        task_receiver = plumpy.ProcessLauncher(loop=loop, persister=persister)

        def callback(*args, **kwargs):
            fut = plumpy.create_task(functools.partial(task_receiver, *args, **kwargs), loop=loop)
            return fut

        comm.add_task_subscriber(callback)

        process_controller = plumpy.RemoteProcessThreadController(comm)

        future = process_controller.launch_process(DummyProcessWithOutput)
        while not future.done():
            pass

        async def task():
            result = await asyncio.wrap_future(future.result())
            print(result)

        loop.run_until_complete(task())


if __name__ == '__main__':
    main()
