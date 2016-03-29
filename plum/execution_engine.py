
from abc import ABCMeta, abstractmethod


class ExecutionEngine(object):
    """
    An execution engine is used to launch Processes.  This interface defines
    the things that the engine must be able to do.
    There are many imaginable types of engine e.g. multithreaded, celery based
    distributed across multiple machines, etc.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def run(self, process):
        pass


class SerialEngine(ExecutionEngine):
    """
    The simplest possible workflow engine.  Just calls through to the run
    method of the process.
    """
    def run(self, process):
        return process.run(self)
