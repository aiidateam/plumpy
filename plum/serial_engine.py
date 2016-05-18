# -*- coding: utf-8 -*-

from plum.execution_engine import ExecutionEngine, Future
from plum.process import ProcessListener
from plum.wait import WaitOn
import time
import uuid


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
        self._do_run(process, inputs)
        return process.get_last_outputs()

    def get_pid(self, process):
        for pid, info in self._current_processes.iteritems():
            if info.process is process:
                return pid
        return None

    def get_process(self, pid):
        return self._current_processes[pid].process

    def tick(self):
        for proc_info in [proc_info for pid, proc_info in
                       self._current_processes.iteritems() if
                       proc_info.waiting_on]:
            if proc_info.waiting_on.is_ready():
                self._continue_process(proc_info)

    ## Process messages #################################################
    def on_process_starting(self, process, inputs):
        super(SerialEngine, self).on_process_starting(process, inputs)
        if self._persistence:
            record = self._persistence.create_running_process_record(
                process, inputs, self.get_pid(process))
            record.save()

    def on_process_waiting(self, process):
        super(SerialEngine, self).on_process_waiting(process)
        if self._persistence:
            proc_info = self._get_proc_info(process)
            record = self._persistence.get_record(proc_info.pid)
            record.create_checkpoint(self, process, proc_info.waiting_on)
            record.save()

    def on_process_finalising(self, process):
        super(SerialEngine, self).on_process_finalising(process)
        if self._persistence:
            self._persistence.delete_record(self.get_pid(process))

    def on_process_finished(self, process, retval):
        super(SerialEngine, self).on_process_finished(process, retval)
        self._remove_process(self.get_pid(process))
    ######################################################################

    def _get_proc_info(self, process):
        for pid, info in self._current_processes.iteritems():
            if process is info.process:
                return info
        return None

    def _do_run(self, process, inputs):
        self._start_process(process, inputs)

        proc_info = self._get_proc_info(process)
        while proc_info.waiting_on:
            # Keep polling until the thing it's waiting for is ready
            while not proc_info.waiting_on.is_ready():
                time.sleep(self._poll_interval)

            self._continue_process(proc_info)

        self._finish_process(proc_info, None)

    def _start_process(self, process, inputs):
        self._add_process(process, self._create_pid())

        ins = process._create_input_args(inputs)
        process.on_start(ins, self)
        retval = process._run(**inputs)

        if isinstance(retval, WaitOn):
            self._wait_process(process, retval)
        else:
            self._finish_process(process, retval)

    def _wait_process(self, process, wait_on):
        self._get_proc_info(process).waiting_on = wait_on
        process.on_wait()

    def _continue_process(self, proc_info):
        assert proc_info.waiting_on, "Cannot continue a process that was not waiting"

        proc = proc_info.process
        wait_on = proc_info.waiting_on
        proc_info.waiting_on = None
        retval = getattr(proc, wait_on.callback)(wait_on)
        if isinstance(retval, WaitOn):
            proc_info.waiting_on = retval
            proc.on_waiting()
        else:
            self._finish_process(proc_info, retval)

    def _finish_process(self, process, retval):
        proc_info = self._get_proc_info(process)
        assert not proc_info.waiting_on, "Cannot finish a process that is waiting"

        proc_info.process.on_finialise()
        proc_info.process.on_finish(retval)

    def _create_pid(self):
        return uuid.uuid1()

    def _add_process(self, proc, pid):
        info = self.ProcessInfo(proc, pid)
        self._current_processes[pid] = info
        proc.add_process_listener(self)
        return info

    def _remove_process(self, pid):
        entry = self._current_processes.pop(pid)
        entry.process.remove_process_listener(self)

