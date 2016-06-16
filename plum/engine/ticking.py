# -*- coding: utf-8 -*-

import threading
import concurrent.futures
from plum.wait import WaitOn
from plum.engine.execution_engine import ExecutionEngine, Future
from plum.util import override
from enum import Enum


class ProcessStatus(Enum):
    QUEUEING = 0,
    RUNNING = 1,
    WAITING = 2,
    FINISHED = 3,
    FAILED=4


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

        for fn in self._done_callbacks:
            fn(self)

    def process_failed(self, exception):
        self._status = self.Status.FAILED
        self._exception = exception

        try:
            self._condition.set()
        except AttributeError:
            pass

    def cancel(self):
        self._engine.cancel(self._pid)
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

        return self._result

    def exception(self, timeout=None):
        if self._status is self.Status.CURRENT:
            self._condition = threading.Event()
            if not self._condition.wait(timeout):
                self._condition = None
                raise concurrent.futures.TimeoutError()
            self._condition = None

        return self._exception

    def add_done_callback(self, fn):
        self._done_callbacks.append(fn)


class TickingEngine(ExecutionEngine):
    class ProcessInfo(object):
        def __init__(self, process, future, status, wait_on=None):
            self._process = process
            self.waiting_on = wait_on
            self.future = future
            self.status = status

        @property
        def process(self):
            return self._process

    def __init__(self, process_factory=None, process_registry=None):
        if process_factory is None:
            from plum.simple_factory import SimpleFactory
            process_factory = SimpleFactory()
        if process_registry is None:
            from plum.simple_registry import SimpleRegistry
            process_registry = SimpleRegistry()

        self._process_factory = process_factory
        self._process_registry = process_registry
        self._current_processes = {}
        self._process_queue = []

    @override
    def submit(self, process_class, inputs):
        process = self._process_factory.create_process(process_class, inputs)
        fut = _Future(self, process.pid)

        # Register before putting in queue because we may be being ticked from
        # anther thread
        if self._process_registry:
            self._process_registry.register_running_process(process)

        # Put it in the queue
        self._current_processes[process.pid] =\
            self.ProcessInfo(process, fut, ProcessStatus.QUEUEING)

        return fut

    @override
    def run_from(self, checkpoint):
        process, wait_on = self._process_factory.recreate_process(checkpoint)
        fut = _Future(self, process.pid)

        # Register before putting in queue because we may be being ticked from
        # anther thread
        if self._process_registry:
            self._process_registry.register_running_process(process)

        # Put it in the queue
        if wait_on:
            self._current_processes[process.pid] =\
                self.ProcessInfo(process, fut, ProcessStatus.WAITING, wait_on)
        else:
            self._current_processes[process.pid] =\
                self.ProcessInfo(process, fut, ProcessStatus.QUEUEING)

        return fut

    def tick(self):
        for proc_info in self._current_processes.values():
            process = proc_info.process

            if proc_info.status is ProcessStatus.QUEUEING:
                self._start_process(proc_info)

            elif proc_info.status is ProcessStatus.WAITING:
                if proc_info.waiting_on.is_ready(self._process_registry):
                    try:
                        self._continue_process(proc_info)
                    except BaseException as e:
                        self._fail_process(proc_info, e)
                        process.signal_on_destroy()
                        proc_info.future.process_failed(e)

            elif proc_info.status is ProcessStatus.FINISHED:
                proc_info.future.process_finished(process.get_last_outputs())
                process.signal_on_destroy()
                del self._current_processes[process.pid]

            else:
                raise RuntimeError(
                    "Process should not be in state {}".format(proc_info.status)
                )

        return len(self._current_processes) > 0

    def cancel(self, pid):
        proc_info = self._current_processes[pid]
        if proc_info.status is ProcessStatus.QUEUEING:
            del self._current_processes[pid]
        else:
            # TODO: Queue up for cancelling next time
            pass

    def _start_process(self, proc_info):
        """
        Send the appropriate messages and start the Process.
        :param proc_info: The process information
        :return: None if the Process is waiting on something, the return value otherwise,
        :note: Do not use a return value of None from this function to indicate that process
        is not waiting on something as the process may simply have returned None.  Instead
        use proc_info.waiting_on is None.
        """
        assert proc_info.status is ProcessStatus.QUEUEING

        process = proc_info.process

        process.signal_on_start(self, self._process_registry)
        retval = process.do_run()
        if isinstance(retval, WaitOn):
            self._wait_process(proc_info, retval)
        else:
            self._finish_process(proc_info, retval)

    def _continue_process(self, proc_info):
        assert proc_info.status is ProcessStatus.WAITING
        assert proc_info.waiting_on,\
            "Cannot continue a process that was not waiting"

        process = proc_info.process

        # Get the WaitOn callback function name and call it
        # making sure to reset the waiting_on
        wait_on = proc_info.waiting_on
        proc_info.waiting_on = None

        process.signal_on_continue(wait_on)
        retval = getattr(process, wait_on.callback)(wait_on)

        # Check what to do next
        if isinstance(retval, WaitOn):
            self._wait_process(proc_info, retval)
        else:
            self._finish_process(proc_info, retval)

    def _wait_process(self, proc_info, wait_on):
        assert not proc_info.waiting_on,\
            "Cannot wait on a process that is already waiting"

        process = proc_info.process

        proc_info.waiting_on = wait_on
        process.signal_on_wait(wait_on)

        proc_info.status = ProcessStatus.WAITING

    def _finish_process(self, proc_info, retval):
        assert not proc_info.waiting_on,\
            "Cannot finish a process that is waiting"

        proc_info.process.signal_on_finish(retval)
        proc_info.status = ProcessStatus.FINISHED
        proc_info.process.signal_on_stop()

    def _fail_process(self, proc_info, exception):
        proc_info.process.signal_on_fail(exception)
        proc_info.status = ProcessStatus.FAILED
        proc_info.process.signal_on_stop()

