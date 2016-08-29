# -*- coding: utf-8 -*-

import sys
from plum.engine.execution_engine import ExecutionEngine, Future
from plum.process import Process
from plum.util import override


class SerialEngine(ExecutionEngine):
    """
    The simplest possible workflow engine.  Just calls through to the run
    method of the process.
    """

    class Future(Future):
        def __init__(self, func, process, *args, **kwargs):
            self._exception = None
            self._result = None
            self._process = process

            # Run the damn thing
            try:
                func(process, *args, **kwargs)
                self._set_result(process.outputs)
            except KeyboardInterrupt:
                # If the user interuppted the process then we should just raise
                # not, not wait around for the process to finish
                raise
            except BaseException:
                exc_obj, exc_tb = sys.exc_info()[1:]
                self._set_exception_info(exc_obj, exc_tb)

        @property
        def process(self):
            return self._process

        @property
        def pid(self):
            return self._process.pid

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
                raise type(self._exception), self._exception, self._traceback
            else:
                return self._result

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

        def _set_exception_info(self, exception, traceback):
            """
            Sets the result of the future as being the given exception
            and traceback.
            """
            self._exception = exception
            self._traceback = traceback

        def _set_result(self, result):
            """Sets the return value of work associated with the future.

            Should only be used by Executor implementations and unit tests.
            """
            self._result = result

    def __init__(self, poll_interval=10):
        self._poll_interval = poll_interval

    @override
    def run(self, process):
        return SerialEngine.Future(Process.run_until_complete, process)

    def run_and_block(self, process_class, inputs):
        """
        Run a process with some inputs immediately.

        :param process_class: The process to execute
        :param inputs: The inputs to execute the process with
        :return: The outputs dictionary from the Process.
        """
        return self.submit(process_class, inputs).result()

    def run_from_and_block(self, checkpoint):
        """
        Run a process with some inputs immediately.

        :param checkpoint: Continue the process from this checkpoint.
        :return: The outputs dictionary from the Process.
        """
        return self.run_from(checkpoint).result()

    @override
    def shutdown(self):
        pass

    def stop(self, pid):
        pass



