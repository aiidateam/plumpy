# -*- coding: utf-8 -*-

from plum.execution_engine import ExecutionEngine, Future
from plum.process import ProcessListener
from plum.wait import WaitOn
import time


class SerialEngine(ExecutionEngine, ProcessListener):
    """
    The simplest possible workflow engine.  Just calls through to the run
    method of the process.
    """

    class Future(Future):
        def __init__(self, process, inputs, engine):
            import sys

            self._exception = None
            self._outputs = None

            # Run the damn thing
            try:
                self._outputs = engine.run(process, inputs)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                self._exception = e

        def cancel(self):
            """
            Always returns False, can't cancel a serial process.

            :return:False
            """
            return False

        def cancelled(self):
            """
            Always False, can't cancel a serial process.

            :return: False
            """
            return False

        def running(self):
            """
            Always False, process is always finished by creation time.

            :return:
            """
            return False

        def done(self):
            """
            Always True, process is always done by creation time.

            :return: True
            """
            return True

        def result(self, timeout=None):
            return self._outputs

        def exception(self, timeout=None):
            return self._exception

        def add_done_callback(self, fn):
            """
            Immediately calls fn because a serial execution is always finished
            by the time this object is created.
            :param func: The function to call
            """
            fn(self)

    _pids = 0

    class ProcessInfo(object):
        def __init__(self, process, pid):
            self.process = process
            self.pid = pid
            self.waiting_on = None

    def __init__(self, poll_interval=10, persistence=None):
        self._poll_interval = poll_interval
        self._current_processes = {}
        self._wait_on_queue = []
        self._persistence = persistence

    def submit(self, process, inputs):
        """
        Submit a process, this gets executed immediately and in fact the Future
        will always be done when returned.

        :param process: The process to execute
        :param inputs: The inputs to execute the process with
        :return: A Future object that represents the execution of the Process.
        """
        return SerialEngine.Future(process, inputs, self)

    def run(self, process, inputs):
        """
        Run a process with some inputs immediately.

        :param process: The process to execute
        :param inputs: The inputs to execute the process with
        :return: A Future object that represents the execution of the Process.
        """
        self._start_process(process, inputs)
        return process.get_last_outputs()

    def get_pid(self, process):
        for pid, info in self._current_processes.iteritems():
            if info.process is process:
                return pid
        return None

    def get_process(self, pid):
        return self._current_processes[pid].process

    def tick(self):
        for record in [record for pid, record in
                       self._current_processes.iteritems() if
                       record.waiting_on]:
            if record.waiting_on.is_ready():
                self._continue_process(record)

    ## Process messages #################################################
    def on_process_starting(self, process, inputs):
        super(SerialEngine, self).on_process_starting(process, inputs)
        if self._persistence:
            self._persistence.create_running_process(
                process, inputs, self.get_pid(process))

    def on_process_finalising(self, process):
        super(SerialEngine, self).on_process_finalising(process)
        if self._persistence:
            self._persistence.delete_record(self.get_pid(process))

    def on_process_finished(self, process, retval):
        super(SerialEngine, self).on_process_finished(process, retval)
        self._remove_process(self.get_pid(process))
    ######################################################################

    def _get_record(self, process):
        for pid, record in self._current_processes.iteritems():
            if process is record.process:
                return record
        return None

    def _do_run(self, process, inputs):
        self._start_process(process, inputs)

        record = self._get_record(process)
        while record.waiting_on:
            # Keep polling until the thing it's waiting for is ready
            while not record.waiting_on.is_ready():
                time.sleep(self._poll_interval)

            self._continue_process(record)

        self._finish_process(record, None)

    def _start_process(self, process, inputs):
        self._add_process(process, self._create_pid())

        ins = process._create_input_args(inputs)
        process._on_process_starting(ins, self)
        retval = process._run(**inputs)

        record = self._get_record(process)
        if isinstance(retval, WaitOn):
            record.waiting_on = retval
        else:
            self._finish_process(record, retval)

    def _continue_process(self, record):
        assert record.waiting_on, "Cannot continue a process that was not waiting"

        proc = record.process
        wait_on = record.waiting_on
        record.waiting_on = None
        retval = getattr(proc, record.waiting_on.callback)(wait_on)
        if isinstance(retval, WaitOn):
            record.waiting_on = retval
        else:
            self._finish_process(record, retval)

    def _finish_process(self, record, retval):
        assert not record.waiting_on, "Cannot finish a process that is waiting"

        record.process._on_process_finalising()
        record.process._on_process_finished(retval)

    def _create_pid(self):
        self._pids += 1
        return self._pids

    def _add_process(self, proc, pid):
        info = self.ProcessInfo(proc, pid)
        self._current_processes[pid] = info
        proc.add_process_listener(self)
        return info

    def _remove_process(self, pid):
        entry = self._current_processes.pop(pid)
        entry.process.remove_process_listener(self)

