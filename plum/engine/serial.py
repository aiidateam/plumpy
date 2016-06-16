# -*- coding: utf-8 -*-

import time

from plum.engine.execution_engine import ExecutionEngine, Future
from plum.util import override
from plum.wait import WaitOn


class SerialEngine(ExecutionEngine):
    """
    The simplest possible workflow engine.  Just calls through to the run
    method of the process.
    """

    class Future(Future):
        def __init__(self, func, process, *args, **kwargs):
            import sys

            self._exception = None
            self._outputs = None
            self._pid = process.pid

            # Run the damn thing
            try:
                self._outputs = func(process, *args, **kwargs)
            except KeyboardInterrupt:
                # If the user interuppted the process then we should just raise
                # not, not wait around for the process to finish
                raise
            except BaseException as e:
                import traceback
                exc_type, exc_obj, exc_tb = sys.exc_info()
                #traceback.print_exc()
                self._exception = e

        @property
        def pid(self):
            return self._pid

        @override
        def cancel(self):
            """
            Always returns False, can't cancel a serial process.

            :return:False
            """
            return False

        @override
        def cancelled(self):
            """
            Always False, can't cancel a serial process.

            :return: False
            """
            return False

        @override
        def running(self):
            """
            Always False, process is always finished by creation time.

            :return:
            """
            return False

        @override
        def done(self):
            """
            Always True, process is always done by creation time.

            :return: True
            """
            return True

        @override
        def result(self, timeout=None):
            if self._exception:
                raise self._exception

            return self._outputs

        @override
        def exception(self, timeout=None):
            return self._exception

        @override
        def add_done_callback(self, func):
            """
            Immediately calls fn because a serial execution is always finished
            by the time this object is created.
            :param func: The function to call
            """
            func(self)

    def __init__(self, poll_interval=10, process_factory=None,
                 process_registry=None):
        if process_factory is None:
            from plum.simple_factory import SimpleFactory
            process_factory = SimpleFactory()
        if process_registry is None:
            from plum.simple_registry import SimpleRegistry
            process_registry = SimpleRegistry()

        self._process_factory = process_factory
        self._process_registry = process_registry
        self._poll_interval = poll_interval

    @override
    def submit(self, process_class, inputs):
        """
        Submit a process, this gets executed immediately and in fact the Future
        will always be done when returned.

        :param process_class: The process to execute
        :param inputs: The inputs to execute the process with
        :return: A Future object that represents the execution of the Process.
        """
        proc = self._process_factory.create_process(process_class, inputs)
        return SerialEngine.Future(self._do_run_and_block, proc)

    def run_and_block(self, process_class, inputs):
        """
        Run a process with some inputs immediately.

        :param process_class: The process to execute
        :param inputs: The inputs to execute the process with
        :return: The outputs dictionary from the Process.
        """
        if inputs is None:
            inputs = {}

        proc = self._process_factory.create_process(process_class, inputs)
        return self._do_run_and_block(proc)

    def _do_run_and_block(self, proc):
        if self._process_registry:
            self._process_registry.register_running_process(proc)
        return self._run_lifecycle(proc)

    @override
    def run_from(self, checkpoint):
        """
        Run a process with some inputs immediately.

        :param checkpoint: Continue the process from this checkpoint.
        :return: A Future object that represents the execution of the Process.
        """
        proc, wait_on = self._process_factory.recreate_process(checkpoint)
        return SerialEngine.Future(
            self.run_from_and_block, proc, checkpoint, wait_on)

    def run_from_and_block(self, checkpoint):
        """
        Run a process with some inputs immediately.

        :param checkpoint: Continue the process from this checkpoint.
        :return: The outputs dictionary from the Process.
        """
        proc, wait_on = self._process_factory.recreate_process(checkpoint)
        return self._do_run_from_and_block(proc, wait_on)

    def _do_run_from_and_block(self, proc, wait_on):
        if self._process_registry:
            self._process_registry.register_running_process(proc)
        return self._run_lifecycle(proc, wait_on)

    def _run_lifecycle(self, proc, wait_on=None):
        """
        Run the process through its events lifecycle.

        :param proc: The process.
        :param wait_on: An optional wait on for the process.
        :return: The outputs dictionary from the process.
        """
        try:
            if wait_on is None:
                self._do_run(proc)
            else:
                self._do_continue(proc, wait_on)
        except BaseException as e:
            # Ok, something has gone wrong with the process (or we've caused
            # an exception).  So, wrap up the process and propagate the
            # exception
            self._fail_process(proc, e)
            proc.signal_on_destroy()
            raise

        outs = proc.get_last_outputs()
        proc.signal_on_destroy()
        return outs

    def _do_run(self, process):
        # Run the process
        retval = self._start_process(process)
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
            self._wait_process(process, wait_on)

            # Keep polling until the thing it's waiting for is ready
            while not wait_on.is_ready(self._process_registry):
                time.sleep(self._poll_interval)

            retval = self._continue_process(process, wait_on)

        return retval

    def _start_process(self, process):
        """
        Send the appropriate messages and start the Process.

        :param process: The process to start
        """
        process.signal_on_start(self, self._process_registry)
        return process.do_run()

    def _continue_process(self, process, wait_on):
        assert wait_on is not None,\
            "Cannot continue a process that was not waiting"

        process.signal_on_continue(wait_on)

        # Get the WaitOn callback function name and call it
        return getattr(process, wait_on.callback)(wait_on)

    def _wait_process(self, process, wait_on):
        assert wait_on is not None,\
            "Cannot wait on a process that is already waiting"

        process.signal_on_wait(wait_on)

    def _finish_process(self, process, retval):
        process.signal_on_finish(retval)
        process.signal_on_stop()

    def _fail_process(self, process, exception):
        process.signal_on_fail(exception)
        process.signal_on_stop()



