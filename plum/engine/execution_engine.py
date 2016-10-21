
from abc import ABCMeta, abstractmethod, abstractproperty
from plum.process import Process


class Future(object):
    __metaclass__ = ABCMeta

    @abstractproperty
    def process(self):
        """
        Get the Process associated with this future.
        :return: The process.
        """
        pass

    @abstractproperty
    def pid(self):
        """
        Get the pid of the Process associated with this future.
        :return:
        """
        pass

    @abstractmethod
    def running(self):
        """
        Return True if the Process is currently being executed.
        :return: True if running, False otherwise.
        """
        pass

    @abstractmethod
    def done(self):
        """
        Return True if the Process was successfully cancelled or finished
        running.
        """
        pass

    @abstractmethod
    def result(self, timeout=None):
        """
        Return the outputs from the Process. If the Process hasn't yet
        completed then this method will wait up to timeout seconds. If the call
        hasn't completed in timeout seconds, then a
        concurrent.futures.TimeoutError will be raised. timeout can be an int
        or float. If timeout is not specified or None, there is no limit to the
        wait time.

        If the future is cancelled before completing then CancelledError will be
        raised.

        If the Process raised, this method will raise the same exception.

        :param timeout: The timeout to wait for.  If None then waits until
        completion.
        """
        pass

    @abstractmethod
    def exception(self, timeout=None):
        """
        Return the exception raised by the Process. If the call hasn't yet
        completed then this method will wait up to timeout seconds. If the
        Process hasn't completed in timeout seconds, then a
        concurrent.futures.TimeoutError will be raised. timeout can be an int
        or float. If timeout is not specified or None, there is no limit to
        the wait time.

        If the future is cancelled before completing then CancelledError will be
        raised.

        If the call completed without raising, None is returned.

        :param timeout: The timeout to wait for.  If None then waits until
        completion.
        """
        pass

    @abstractmethod
    def add_done_callback(self, fn):
        """
        Attaches the callable fn to the future. fn will be called, with the
        future as its only argument, when the future is cancelled or finishes
        running.

        Added callables are called in the order that they were added and are
        always called in a thread belonging to the process that added them. If
        the callable raises an Exception subclass, it will be logged and
        ignored. If the callable raises a BaseException subclass, the behavior
        is undefined.

        If the future has already completed or been cancelled, fn will be
        called immediately.

        :param func: The function to call back.
        """
        pass


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
        """
        Run an instance of an existing process.  The engine takes ownership
        thus the process must not be being ran by another engine or being
        ticked by the user.

        :param process: The process to run
        :type process: :class:`Process`
        :return: A future that represents the execution of the process.
        :rtype: :class:`Future`
        """
        pass

    def submit(self, process_class, inputs=None):
        """
        Submit a process to be executed with some inputs at some point.

        :param process_class: The process class to execute
        :param inputs: The inputs to execute the process with
        :return: A future that represents the execution of the process.
        :rtype: :class:`Future`
        """
        return self.run(process_class.new_instance(inputs))

    def run_from(self, checkpoint):
        """
        Run a process from the given checkpoint.

        :param checkpoint: The checkpoint to continue the process from.
        :return: A future that represents the execution of the process.
        :rtype: :class:`Future`
        """
        return self.run(Process.create_from(checkpoint))

    @abstractmethod
    def stop(self, pid):
        """
        Stop a running process.  If a process is in or enters a waiting state it
        will be stopped at this point.  Otherwise the process will continue
        until finished and stop after this.

        :param pid: The pid of the process to stop.
        """
        pass

    @abstractmethod
    def shutdown(self):
        """
        Shut down the engine cancelling and destroying all current processes.
        This could take some time depending on what the processes are doing and
        the exect details of the engine implementation.
        """
        pass
