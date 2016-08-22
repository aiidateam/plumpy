import multiprocessing

import concurrent.futures
import plum.engine.execution_engine as execution_engine
from plum.process import Process
from plum.process_monitor import ProcessMonitorListener
from concurrent.futures import ThreadPoolExecutor
from plum.util import override


class MultithreadedEngine(execution_engine.ExecutionEngine, ProcessMonitorListener):
    class Future(concurrent.futures.Future, execution_engine.Future):
        def __init__(self, process, future):
            self._process = process
            self._future = future

        @property
        def pid(self):
            return self._process.pid

        @property
        def process(self):
            return self._process

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

    def __init__(self, max_workers=None):
        if max_workers is None:
            max_workers = multiprocessing.cpu_count()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._processes = {}

    @override
    def run(self, process):
        f = self._executor.submit(Process.run_until_complete, process)
        self._processes[process.pid] = f
        return self.Future(process.pid, f)

    @override
    def stop(self, pid):
        self._processes[pid].process.stop()

    @override
    def shutdown(self):
        for pid in self._processes:
            self.stop(pid)

    # From ProcessMonitorListener #############################################
    @override
    def on_monitored_process_destroying(self, process):
        if process.pid in self._processes:
            del self._processes[process.pid]

    @override
    def on_monitored_process_failed(self, pid):
        if pid in self._processes:
            del self._processes[pid]
    ###########################################################################