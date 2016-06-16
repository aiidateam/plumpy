import multiprocessing

import concurrent.futures
import plum.engine.execution_engine as execution_engine
from concurrent.futures import ThreadPoolExecutor
from plum.engine.serial import SerialEngine
from plum.util import override


class MultithreadedEngine(execution_engine.ExecutionEngine):
    class Future(concurrent.futures.Future, execution_engine.Future):
        def __init__(self, pid, future):
            self._pid = pid
            self._future = future

        @property
        def pid(self):
            return self._pid

        @override
        def cancel(self):
            return self._future.cancel()

        @override
        def cancelled(self):
            return self._future.cancelled()

        @override
        def running(self):
            return self._future.running()

        @override
        def done(self):
            return self._future.done()

        @override
        def result(self, timeout=None):
            return self._future.result(timeout)

        @override
        def exception(self, timeout=None):
            return self._future.exception(timeout)

        @override
        def add_done_callback(self, fn):
            self._future.add_done_callback(fn)

    def __init__(self, max_workers=None, process_factory=None,
                 process_registry=None):
        if max_workers is None:
            max_workers = multiprocessing.cpu_count()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        # For now just exploit the serial engine to do the work for us
        self._serial_engine = SerialEngine(
            process_factory=process_factory, process_registry=process_registry)

    @override
    def submit(self, process_class, inputs):
        """
        Submit a process to be executed by a separate thread at some point.

        :param process_class: The process to execute
        :param inputs: The inputs to execute the process with
        :return: A Future object that represents the execution of the Process.
        """
        p = self._serial_engine._process_factory.create_process(
            process_class, inputs)
        f = self._executor.submit(self._serial_engine._do_run_and_block, p)
        return self.Future(p.pid, f)

    @override
    def run_from(self, checkpoint):
        """
        Submit a process to be continued by a separate thread at some point.

        :param checkpoint: The checkpoint to continue from.
        :return: A Future object that represents the execution of the Process.
        """
        return self._executor.submit(self._serial_engine.run_from_and_block,
                                     checkpoint)
