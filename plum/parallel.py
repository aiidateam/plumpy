
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import plum.execution_engine as execution_engine
from plum.serial_engine import SerialEngine
from plum.util import override


class MultithreadedEngine(execution_engine.ExecutionEngine):
    class Future(concurrent.futures.Future, execution_engine.Future):
        pass

    def __init__(self, max_workers=None, process_manager=None):
        if max_workers is None:
            max_workers = multiprocessing.cpu_count()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        # For now just exploit the serial engine to do the work for us
        self._serial_engine = SerialEngine(process_manager=process_manager)

    @override
    def submit(self, process_class, inputs, checkpoint=None):
        """
        Submit a process to be executed by a separate thread at some point

        :param process_class: The process to execute
        :param inputs: The inputs to execute the process with
        :return: A Future object that represents the execution of the Process.
        """
        return self._executor.submit(self._serial_engine.run, process_class,
                                     inputs, checkpoint)
