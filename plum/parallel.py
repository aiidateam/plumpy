import multiprocessing

import concurrent.futures
from plum.process import Process
from plum.process_monitor import ProcessMonitorListener
from concurrent.futures import ThreadPoolExecutor
from plum.util import override


class MultithreadedEngine(ProcessMonitorListener):
    class Future(concurrent.futures.Future):
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

    def submit(self, ProcClass, inputs=None):
        return self.start(ProcClass.new_instance(inputs))

    def start(self, process):
        f = self._executor.submit(Process.start, process)
        self._processes[process.pid] = f
        return self.Future(process, f)

    def abort(self, pid):
        self._processes[pid].process.abort()

    def shutdown(self):
        for pid in self._processes:
            self.abort(pid)

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
