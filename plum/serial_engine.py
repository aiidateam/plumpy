# -*- coding: utf-8 -*-

from plum.execution_engine import ExecutionEngine, Future
from plum.wait import WaitOn
import time


class SerialEngine(ExecutionEngine):
    """
    The simplest possible workflow engine.  Just calls through to the run
    method of the process.
    """

    class Future(Future):
        def __init__(self, engine, process, inputs, checkpoint):
            import sys

            self._exception = None
            self._outputs = None

            # Run the damn thing
            try:
                self._outputs = engine.run(process, inputs, checkpoint)
            except Exception as e:
                import traceback
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
            if self._exception:
                raise self._exception

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

    def __init__(self, poll_interval=10, process_manager=None):
        if process_manager is None:
            from plum.simple_manager import SimpleManager
            process_manager = SimpleManager()

        self._process_manager = process_manager
        self._poll_interval = poll_interval

    def submit(self, process_class, inputs, checkpoint=None):
        """
        Submit a process, this gets executed immediately and in fact the Future
        will always be done when returned.

        :param process_class: The process to execute
        :param inputs: The inputs to execute the process with
        :param checkpoint: If supplied will continue the process from this
        checkpoint instead of starting from the beginning.
        :return: A Future object that represents the execution of the Process.
        """
        return SerialEngine.Future(self, process_class, inputs, checkpoint)

    def run(self, process_class, inputs, checkpoint=None):
        """
        Run a process with some inputs immediately.

        :param process_class: The process to execute
        :param inputs: The inputs to execute the process with
        :param checkpoint: If supplied will continue the process from this
        checkpoint instead of starting from the beginning.
        :return: A Future object that represents the execution of the Process.
        """
        if inputs is None:
            inputs = {}

        proc, wait_on =\
            self._process_manager.create_process(process_class, checkpoint)

        if wait_on is None:
            self._do_run(proc, inputs)
        else:
            self._do_continue(proc, wait_on)

        return proc.get_last_outputs()

    def _do_run(self, process, inputs):
        # Run the process
        retval = self._start_process(process, inputs)
        if isinstance(retval, WaitOn):
            retval = self._continue_till_finished(process, retval)
        self._finish_process(process, retval)

    def _do_continue(self, process, wait_on):
        retval = self._continue_till_finished(process, wait_on)
        self._finish_process(process, retval)

    def _continue_till_finished(self, process, wait_on):
        # Keep looping until there is nothing to wait for
        retval = wait_on
        while isinstance(retval, WaitOn):
            # Keep polling until the thing it's waiting for is ready
            while not wait_on.is_ready():
                time.sleep(self._poll_interval)

            retval = self._continue_process(process, wait_on)

        return retval

    def _start_process(self, process, inputs):
        """
        Send the appropriate messages and start the Process.

        :param proc_info: The process information
        :return: None if the Process is waiting on something, the return value otherwise,
        :note: Do not use a return value of None from this function to indicate that process
        is not waiting on something as the process may simply have returned None.  Instead
        use proc_info.waiting_on is None.
        """
        ins = process._create_input_args(inputs)
        process.on_start(ins, self)

        return process._run(**inputs)

    def _continue_process(self, process, wait_on):
        assert wait_on is not None,\
            "Cannot continue a process that was not waiting"

        process.on_continue(wait_on)

        # Get the WaitOn callback function name and call it
        return getattr(process, wait_on.callback)(wait_on)

    def _wait_process(self, process, wait_on):
        assert wait_on is not None,\
            "Cannot wait on a process that is already waiting"

        process.on_wait()

    def _finish_process(self, process, retval):
        process.on_finish(retval)


