
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import plum.execution_engine as execution_engine
from plum.serial_engine import SerialEngine


class MultithreadedExecutionEngine(execution_engine.ExecutionEngine):
    class Future(concurrent.futures.Future, execution_engine.Future):
        pass

    def __init__(self, max_workers=None, persistence=None):
        if max_workers is None:
            max_workers = multiprocessing.cpu_count()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        # For now just exploit the serial engine to do the work for us
        self._serial_engine = SerialEngine(persistence=persistence)

    def submit(self, process, inputs):
        """
        Submit a process to be executed by a separate thread at some point

        :param process: The process to execute
        :param inputs: The inputs to execute the process with
        :return: A Future object that represents the execution of the Process.
        """
        return self._executor.submit(self._serial_engine.run, process, inputs)

    def run(self, process, inputs):
        """
        Run a process with some inputs immediately.

        :param process: The process to execute
        :param inputs: The inputs to execute the process with
        :return:
        """
        return self._serial_engine.run(process, inputs)

    def tick(self):
        self._serial_engine.tick()

    def get_pid(self, process):
        return self._serial_engine.get_pid(process)

    def get_process(self, pid):
        return self._serial_engine.get_process(pid)
