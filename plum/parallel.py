
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import plum.execution_engine as execution_engine
from plum.wait import WaitOn


class MultithreadedExecutionEngine(execution_engine.ExecutionEngine):
    class Future(concurrent.futures.Future, execution_engine.Future):
        pass

    def __init__(self, max_workers=None):
        if max_workers is None:
            max_workers = multiprocessing.cpu_count()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def submit(self, process, inputs):
        """
        Submit a process to be executed by a separate thread at some point

        :param process: The process to execute
        :param inputs: The inputs to execute the process with
        :return: A Future object that represents the execution of the Process.
        """
        return self._executor.submit(self.run, process, inputs)

    def run(self, process, inputs):
        """
        Run a process with some inputs immediately.

        :param process: The process to execute
        :param inputs: The inputs to execute the process with
        :return:
        """
        self._run_till_finished(process, inputs)
        return process.get_last_outputs()

    def _run_till_finished(self, process, inputs):
        ins = process._create_input_args(inputs)

        process._on_process_starting(ins, self)

        retval = process._run(**inputs)
        while isinstance(retval, WaitOn):
            retval.wait(timeout=WaitOn.UNTIL_READY)
            retval = retval.callback()

        process._on_process_finalising()

        return retval
