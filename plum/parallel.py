
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import plum.execution_engine as execution_engine


class MultithreadedExecutionEngine(execution_engine.ExecutionEngine):
    class Future(concurrent.futures.Future, execution_engine.Future):
        pass

    def __init__(self):
        self._executor = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())

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
        process.run(inputs, self)
        return process.get_last_outputs()


