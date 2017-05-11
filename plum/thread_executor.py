import threading
import time
from concurrent.futures import ThreadPoolExecutor as PythonThreadPoolExecutor
from plum.process import Process, ProcessListener
from plum.util import protected
from plum.exceptions import TimeoutError


class _ProcInfo(object):
    def __init__(self, proc, thread):
        self.proc = proc
        self.executor_future = None


class Future(ProcessListener):
    def __init__(self, process, procman):
        """
        The process manager creates instances of futures that can be used by the
        user.

        :param process: The process this is a future for
        :type process: :class:`plum.process.Process`
        """
        self._process = process
        self._procman = procman
        self._terminated = threading.Event()
        self._process.add_process_listener(self)
        self._callbacks = []
        if self._process.has_terminated():
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
        if self._terminated.wait(timeout):
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
        return self._process.abort(msg, timeout)

    def play(self):
        return self._procman.play(self._process)

    def pause(self, timeout):
        return self._process.pause(timeout)

    def wait(self, timeout=None):
        return self._terminated.wait(timeout)

    def add_done_callback(self, fn):
        if self._terminated.is_set():
            fn(self)
        else:
            self._callbacks.append(fn)

    @protected
    def on_process_done_playing(self, process):
        if process.has_terminated():
            self._terminate()

    def _terminate(self):
        self._terminated.set()
        for fn in self._callbacks:
            fn(self)


def wait_for_all(futures):
    for future in futures:
        future.wait()


class PlayError(Exception):
    pass


class ThreadPoolExecutor(ProcessListener):
    """
    Used to launch processes on separate threads and monitor their progress
    """

    def __init__(self, max_threads=1024):
        self._max_threads = max_threads
        self._processes = {}
        self._executor = PythonThreadPoolExecutor(max_workers=self._max_threads)

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
        self._processes[proc.pid] = proc
        proc.add_process_listener(self)
        self._play(proc)
        return Future(proc, self)

    def get_processes(self):
        return self._processes.values()

    def has_process(self, pid):
        return pid in self._processes

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

    def get_num_processes(self):
        return len(self._processes)

    @protected
    def on_process_done_playing(self, process):
        process.remove_process_listener(self)
        self._processes.pop(process.pid)

    def _play(self, proc):
        if not proc.is_playing():
            future = self._executor.submit(proc.play)
            for i in range(0, 100):
                if future.running():
                    return
                time.sleep(0.01)
            if future.cancel():
                raise PlayError("Couldn't play the process, probably out of workers")


_DEFAULT_PROCMAN = None


def get_default_procman():
    """
    :return: The default process manager
    :rtype: :class:`ThreadPoolExecutor`
    """
    global _DEFAULT_PROCMAN
    if _DEFAULT_PROCMAN is None:
        _DEFAULT_PROCMAN = ThreadPoolExecutor()
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
