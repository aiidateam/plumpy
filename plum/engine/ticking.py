# -*- coding: utf-8 -*-

import sys
import threading
import concurrent.futures
import uuid
from plum.process import Process, ProcessState
from plum.engine.execution_engine import ExecutionEngine, Future
from plum.util import override
from enum import Enum


class _Future(Future):
    class Status(Enum):
        CURRENT = 0
        CANCELLED = 1
        FINISHED = 2
        FAILED = 3

    def __init__(self, engine, pid):
        self._engine = engine
        self._pid = pid
        self._status = self.Status.CURRENT
        self._done_callbacks = []
        self._result = None
        self._condition = None
        self._exception = None
        self._traceback = None

    @property
    def pid(self):
        return self._pid

    def process_finished(self, result):
        self._status = self.Status.FINISHED
        self._result = result

        try:
            self._condition.set()
        except AttributeError:
            pass

        self._invoke_callbacks()

    def process_failed(self, exception, traceback):
        self._status = self.Status.FAILED
        self._exception = exception
        self._traceback = traceback

        try:
            self._condition.set()
        except AttributeError:
            pass

        self._invoke_callbacks()

    def cancel(self):
        self._engine.stop(self._pid)
        self._status = self.Status.CANCELLED

    def cancelled(self):
        return self._status is self.Status.CANCELLED

    def running(self):
        return self._status is self.Status.CURRENT

    def done(self):
        return self._status in [self.Status.CANCELLED, self.Status.FINISHED]

    def result(self, timeout=None):
        if self._status is self.Status.CURRENT:
            self._condition = threading.Event()
            if not self._condition.wait(timeout):
                raise concurrent.futures.TimeoutError()
            self._condition = None

        if self._exception:
            raise type(self._exception), self._exception, self._traceback
        else:
            return self._result

    def exception(self, timeout=None):
        if self._status is self.Status.CURRENT:
            self._condition = threading.Event()
            if not self._condition.wait(timeout):
                self._condition = None
                raise concurrent.futures.TimeoutError()
            self._condition = None

        return self._exception

    def _invoke_callbacks(self):
        for callback in self._done_callbacks:
            try:
                callback(self)
            except Exception:
                # TODO: Log this
                pass

    def add_done_callback(self, fn):
        self._done_callbacks.append(fn)


class TickingEngine(ExecutionEngine):
    class ProcessInfo(object):
        def __init__(self, process, future):
            self._process = process
            self.future = future

        @property
        def process(self):
            return self._process

        @property
        def pid(self):
            return self._process.pid

    def __init__(self):
        self._current_processes = {}
        self._shutting_down = False

    @override
    def submit(self, process_class, inputs=None):
        assert not self._shutting_down

        proc = process_class.create(self._create_pid(), inputs)
        fut = _Future(self, proc.pid)

        # Put it in the queue
        self._current_processes[proc.pid] = self.ProcessInfo(proc, fut)

        return fut

    @override
    def run_from(self, checkpoint):
        assert not self._shutting_down

        proc = Process.create_from(checkpoint)
        fut = _Future(self, proc.pid)
        self._current_processes[proc.pid] = self.ProcessInfo(proc, fut)

        return fut

    @override
    def stop(self, pid):
        self._current_processes[pid].process.stop()

    def shutdown(self):
        """
        Shutdown the ticking engine.  This will stop all processes.  This call
        will block until all processes are cancelled which could take some time
        if there are currently running processes.
        """
        assert not self._shutting_down

        self._shutting_down = True
        for info in self._current_processes.itervalues():
            self.stop(info.pid)

        # This will get the processes to stop and destroy themselves
        for proc_info in self._current_processes:
            proc_info.process.run_till_end()

    def tick(self):
        to_delete = []

        for proc_info in self._current_processes.values():
            proc = proc_info.process
            pid = proc.pid

            # Run the damn thing
            try:
                proc.tick()
            except KeyboardInterrupt:
                # If the user interuppted the process then we should just raise
                # not, not wait around for the process to finish
                raise
            except BaseException:
                exc_obj, exc_tb = sys.exc_info()[1:]
                proc_info.future.process_failed(exc_obj, exc_tb)
                # Process is dead
                to_delete.append(pid)

            if proc.state is ProcessState.FINISHED:
                proc_info.future.process_finished(proc.get_last_outputs())
            elif proc.state is ProcessState.DESTROYED:
                to_delete.append(pid)

        for pid in to_delete:
            del self._current_processes[pid]

        return len(self._current_processes) > 0

    def _create_pid(self):
        return uuid.uuid1()