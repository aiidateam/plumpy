import logging
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor
from plum.process import Process, ProcessListener, ProcessState
from plum.util import protected
from plum.exceptions import TimeoutError

_LOGGER = logging.getLogger(__name__)


class _ProcInfo(object):
    def __init__(self, proc, thread):
        self.proc = proc
        self.executor_future = None


class Future(object):
    def __init__(self, process, procman):
        """
        Executors creates instances of futures that can be used by the to monitor
        the execution of a process.

        :param process: The process this is a future for
        :type process: :class:`plum.process.Process`
        """
        self._process = process
        self._procman = procman
        self._paused = threading.Event()
        self._paused.set()
        self._callbacks = []
        self._terminated = threading.Event()
        if process.has_terminated():
            self._terminated.set()

    @property
    def pid(self):
        """
        Contains the pid of the process

        :return: The pid
        """
        return self._process.pid

    @property
    def outputs(self):
        """
        Contains the current outputs of the process.  If it is still running
        these may grow (but not change) over time.

        :return: A mapping of {output_port: value} outputs
        :rtype: dict
        """
        return self._process.outputs

    def get_process(self):
        return self._process

    def result(self, timeout=None):
        """
        This method will block until the process has finished producing outputs
        and then return the final dictionary of outputs.

        If a timeout is given and the process has not finished in that time
        a :class:`TimeoutError` will be raised.

        :param timeout: (optional) maximum time to wait for process to finish
        :return: The final outputs
        """
        if self.wait(timeout):
            if self._process.has_failed():
                exc_info = self._process.get_exc_info()
                raise exc_info[0], exc_info[1], exc_info[2]
            else:
                return self.outputs
        else:
            raise TimeoutError()

    def exception(self, timeout=None):
        try:
            self.result(timeout)
        except TimeoutError:
            raise
        except BaseException as e:
            return e

        return None

    def abort(self, msg=None, timeout=None):
        """
        Abort the process

        :param msg: The abort message
        :type msg: str
        :param timeout: How long to wait for the process to abort itself
        :type timeout: float
        """
        self._process.abort(msg, timeout=0)
        return self.wait(timeout)

    def play(self):
        return self._procman.do_play(self)

    def pause(self, timeout):
        self._process.pause(timeout=0)
        return self._paused.wait(timeout)

    def wait(self, timeout=None):
        return self._terminated.wait(timeout)

    def add_done_callback(self, fn):
        if self._terminated.is_set():
            fn(self)
        else:
            self._callbacks.append(fn)

    def _playing(self):
        self._paused.clear()

    def _done_playing(self):
        self._paused.set()
        if self._process.has_terminated():
            self._terminated.set()
            for fn in self._callbacks:
                fn(self)


def wait_for_all(futures):
    for future in futures:
        future.wait()


class PlayError(Exception):
    pass


class _ExecutorCommon(object):
    def __init__(self, max_threads):
        self._executor = ThreadPoolExecutor(max_workers=max_threads)
        self._processes = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _LOGGER.debug("Shutting down executor")
        self.shutdown()

    def get_processes(self):
        return self._processes.values()

    def has_process(self, pid):
        return pid in self._processes

    def get_num_processes(self):
        return len(self._processes)

    def pause(self, pid, timeout=None):
        try:
            return self._processes[pid].pause(timeout)
        except KeyError:
            raise ValueError("Unknown pid")

    def pause_all(self, timeout=None):
        """
        Pause all processes.
        """
        num_paused = 0

        time_left = timeout
        t0 = time.time()
        for proc in self._processes.values():
            if proc.pause(timeout=time_left):
                num_paused += 1

            if time_left is not None:
                time_left = timeout - (time.time() - t0)

        return num_paused

    def abort(self, pid, msg=None, timeout=None):
        """
        Abort a process.

        :param pid: The process id
        :param msg: (optional) The abort message
        :param timeout: (optional) Time to wait until aborted
        :return: True if aborted, False otherwise
        """
        try:
            return self._processes[pid].abort(msg, timeout)
        except KeyError:
            raise ValueError("Unknown pid")

    def abort_all(self, msg=None, timeout=None):
        num_aborted = 0

        time_left = timeout
        t0 = time.time()
        for proc in self._processes.values():
            try:
                if proc.abort(msg, time_left):
                    num_aborted += 1
            except AssertionError:
                # This happens if the process is not playing
                pass

            if time_left is not None:
                time_left = timeout - (time.time() - t0)

        return num_aborted

    def shutdown(self, wait=True):
        for proc in self._processes.values():
            proc.abort(timeout=0)
        return self._executor.shutdown(wait)


class ThreadExecutor(_ExecutorCommon, ProcessListener):
    """
    Used to launch processes on separate threads and monitor their progress
    """

    def __init__(self, max_threads=1024):
        super(ThreadExecutor, self).__init__(max_threads)

    def launch(self, proc_class, inputs=None, pid=None, logger=None):
        """
        Create a process and start it.

        :param proc_class: The process class
        :param inputs: The inputs to the process
        :param pid: The (optional) pid for the process
        :param logger: The (optional) logger for the process to use
        :return: A :class:`Future` representing the execution of the process
        :rtype: :class:`Future`
        """
        return self.play(proc_class.new(inputs, pid, logger))

    def play(self, proc):
        """
        Start an existing process.

        :param proc: The process to start
        :type proc: :class:`plum.process.Process`
        :return: A :class:`Future` representing the execution of the process
        :rtype: :class:`Future`
        """
        _LOGGER.debug("Submitting '{}'".format(proc.pid))

        if self.has_process(proc.pid):
            raise ValueError("Cannot play the same process twice")

        future = Future(proc, self)
        self.do_play(future)

        return future

    def do_play(self, future):
        proc = future.get_process()
        proc.add_process_listener(self)
        self._processes[proc.pid] = proc
        self._executor.submit(self._play, future)

    def _play(self, future):
        """
        :param future: The future to play
        :type future: :class:`Future`
        """
        proc = future.get_process()
        future._playing()

        _LOGGER.debug("Playing '{}'".format(proc.pid))
        retval = proc.play()
        _LOGGER.debug("Finished playing '{}'".format(proc.pid))

        self._processes.pop(proc.pid)
        future._done_playing()

        return retval


class SchedulingExecutor(_ExecutorCommon, ProcessListener):
    """
    A scheduling executor that tries to run processes whilst others are waiting. 
    
    Given a maximum number of threads this executor will try to keep as many processes
    running as possible by pausing those that are waiting and allowing others to run
    in the meantime.
    """

    def __init__(self, max_threads):
        super(SchedulingExecutor, self).__init__(max_threads)

        self._max_threads = max_threads
        self._playing = []
        self._waiting_to_requeue = []
        self._state_lock = threading.Lock()

    def launch(self, proc_class, inputs=None, pid=None, logger=None):
        """
        Create a process and start it.

        :param proc_class: The process class
        :param inputs: The inputs to the process
        :param pid: The (optional) pid for the process
        :param logger: The (optional) logger for the process to use
        :return: A :class:`Future` representing the execution of the process
        :rtype: :class:`Future`
        """
        return self.play(proc_class.new(inputs, pid, logger))

    def play(self, proc):
        """
        Start an existing process.

        :param proc: The process to start
        :type proc: :class:`plum.process.Process`
        :return: A :class:`Future` representing the execution of the process
        :rtype: :class:`Future`
        """
        _LOGGER.debug("Submitting '{}'".format(proc.pid))

        if self.has_process(proc.pid):
            raise ValueError("Cannot play the same process twice")

        future = Future(proc, self)
        self._processes[proc.pid] = proc
        proc.add_process_listener(self)
        self.do_play(future)

        if self.get_num_processes() > self._max_threads:
            # Try and pull off a waiting process
            with self._state_lock:
                # Find all those waiting
                waiting = [
                    pid for pid in self._playing if
                    self._processes[pid].state is ProcessState.WAITING
                    ]
                if waiting:
                    proc = self._processes[random.choice(waiting)]
                    self._requeue(proc)

        return future

    def do_play(self, future):
        self._executor.submit(self._play, future)

    @protected
    def on_process_wait(self, process):
        if self.get_num_processes() > self._max_threads:
            # Let someone else have a chance
            self._requeue(process)

    @protected
    def on_process_done_playing(self, proc):
        if proc.has_terminated():
            proc.remove_process_listener(self)
            self._processes.pop(proc.pid)

    def _play(self, future):
        """
        :param future: The future to play
        :type future: :class:`Futrue`
        """
        proc = future.get_process()

        _LOGGER.debug("Playing '{}'".format(proc.pid))
        with self._state_lock:
            self._playing.append(proc.pid)
        future._playing()

        retval = proc.play()

        with self._state_lock:
            self._playing.remove(proc.pid)
        future._done_playing()
        _LOGGER.debug("Finished playing '{}'".format(proc.pid))

        # Done now
        if proc in self._waiting_to_requeue:
            if not proc.has_terminated():
                _LOGGER.debug("Submitting '{}'".format(proc.pid))
                self._executor.submit(self._play, future)
            self._waiting_to_requeue.remove(proc)

        return retval

    def _requeue(self, process):
        _LOGGER.debug("Requeueing '{}'".format(process.pid))
        self._waiting_to_requeue.append(process)
        process.pause(timeout=0)


_DEFAULT_PROCMAN = None


def get_default_procman():
    """
    :return: The default process manager
    :rtype: :class:`ThreadExecutor`
    """
    global _DEFAULT_PROCMAN
    if _DEFAULT_PROCMAN is None:
        _DEFAULT_PROCMAN = ThreadExecutor()
    return _DEFAULT_PROCMAN


def async(process, **inputs):
    if isinstance(process, Process):
        assert not inputs, "Cannot pass inputs to an already instantiated process"
        to_play = process
    elif isinstance(process, Process.__class__):
        to_play = process.new(inputs=inputs)
    else:
        raise ValueError("Process must be a process instance or class")

    return get_default_procman().play(to_play)
