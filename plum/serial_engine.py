# -*- coding: utf-8 -*-

from plum.execution_engine import ExecutionEngine, Future
from plum.wait import WaitOn
import time
import uuid


class SerialEngine(ExecutionEngine):
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

    class ProcessInfo(object):
        @classmethod
        def from_process(cls, pid, process, inputs):
            return cls(pid, process=process, inputs=inputs)

        @classmethod
        def from_record(cls, record):
            return cls(record.pid, record=record)

        def __init__(self, pid, process=None, inputs=None, wait_on=None, record=None):
            self._process = process
            self.inputs = inputs
            self.pid = pid
            self.waiting_on = wait_on
            self.record = record

        @property
        def process(self):
            if self._process is None:
                self._load_process()
            return self._process

        def _load_process(self):
            assert not self.process
            assert (self.record and self.record.has_checkpoint())

            self._process = self.record.create_process_from_checkpoint()
            self.inputs = self.record.inputs

    def __init__(self, poll_interval=10, persistence=None):
        self._poll_interval = poll_interval
        self._current_processes = {}
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
        self._do_run(process, inputs)
        return process.get_last_outputs()

    def run_from(self, process_record):
        assert process_record.has_checkpoint()

        proc_info = self.ProcessInfo.from_record(process_record)
        self._current_processes[proc_info.pid] = proc_info
        proc_info.waiting_on = proc_info.record.create_wait_on_from_checkpoint()
        self._do_continue(proc_info)

    def get_process(self, pid):
        return self._current_processes[pid].process

    def _do_run(self, process, inputs):
        proc_info = self._register_new_process(process, inputs)

        # Run the process
        retval = self._start_process(proc_info)
        self._do_continue(proc_info, retval)

    def _do_continue(self, proc_info, retval=None):
        # Continue the process
        if proc_info.waiting_on:
            retval = self._continue_till_finished(proc_info)
        self._finish_process(proc_info, retval)

        del self._current_processes[proc_info.pid]

    def _register_new_process(self, process, inputs):
        # Set up the process information we need
        pid = self._create_pid()
        proc_info = self.ProcessInfo.from_process(pid, process, inputs)
        self._current_processes[pid] = proc_info
        return proc_info

    def _start_process(self, proc_info):
        """
        Send the appropriate messages and start the Process.
        :param proc_info: The process information
        :return: None if the Process is waiting on something, the return value otherwise,
        :note: Do not use a return value of None from this function to indicate that process
        is not waiting on something as the process may simply have returned None.  Instead
        use proc_info.waiting_on is None.
        """
        process = proc_info.process
        inputs = proc_info.inputs

        ins = process._create_input_args(inputs)
        process.on_start(ins, self)
        if self._persistence:
            record = self._persistence.create_running_process_record(process, inputs, proc_info.pid)
            record.save()
            proc_info.record = record

        retval = process._run(**inputs)
        if isinstance(retval, WaitOn):
            self._wait_process(proc_info, retval)
        else:
            return retval

    def _continue_process(self, proc_info):
        assert proc_info.waiting_on, "Cannot continue a process that was not waiting"

        # Get the WaitOn callback function name and call it
        # making sure to reset the waiting_on
        wait_on = proc_info.waiting_on
        proc_info.waiting_on = None
        retval = getattr(proc_info.process, wait_on.callback)(wait_on)

        # Check what to do next
        if isinstance(retval, WaitOn):
            self._wait_process(proc_info, retval)
        else:
            return retval

    def _continue_till_finished(self, proc_info):
        # Keep lookuping until there is nothing to wait for
        retval = None
        while proc_info.waiting_on:
            # Keep polling until the thing it's waiting for is ready
            while not proc_info.waiting_on.is_ready():
                time.sleep(self._poll_interval)
            retval = self._continue_process(proc_info)
        return retval

    def _wait_process(self, proc_info, wait_on):
        assert not proc_info.waiting_on, "Cannot wait on a process that is already waiting"

        proc_info.waiting_on = wait_on
        proc_info.process.on_wait()
        if proc_info.record:
            proc_info.record.create_checkpoint(self, proc_info.process, proc_info.waiting_on)
            proc_info.record.save()

    def _finish_process(self, proc_info, retval):
        assert not proc_info.waiting_on, "Cannot finish a process that is waiting"

        proc_info.process.on_finalise()
        if proc_info.record:
            proc_info.record.delete(proc_info.pid)
            proc_info.record = None
        proc_info.process.on_finish(retval)

    def _create_pid(self):
        return uuid.uuid1()

