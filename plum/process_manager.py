import threading
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from plum.process import ProcessListener
from plum.util import override, protected
from plum.exceptions import TimeoutError


class _ProcInfo(object):
    def __init__(self, proc, thread):
        self.proc = proc
        self.executor_future = None


class Future(ProcessListener):
    def __init__(self, procman, process):
        """
        The process manager creates instances of futures that can be used by the
        user.

        :param procman: The process manager that the process belongs to
        :type procman: :class:`ProcessManager`
        :param process: The process this is a future for
        :type process: :class:`plum.process.Process`
        """
        self._procman = procman
        self._process = process
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

    def result(self, timeout=None):
        """
        This method will block until the process has finished producing outputs
        and then return the final dictionary of outputs.

        If a timeout is given and the process has not finished in that time
        a :class:`TiemoutError` will be raised.

        :param timeout: (optional) maximum time to wait for process to finish
        :return: The final outputs
        """
        if self._terminated.wait(timeout):
            if self._process.has_failed():
                # TODO: Get the original traceback, requires it to be stored in process
                raise self._process.get_exception()
            else:
                return self.outputs
        else:
            raise TimeoutError()

    def abort(self, msg=None, timeout=None):
        return self._procman.abort(self.pid, msg, timeout)

    def play(self):
        return self._procman.play(self.pid)

    def pause(self, timeout):
        return self._procman.pause(self.pid, timeout)

    def wait(self, timeout=None):
        try:
            return self._procman.wait_for(self.pid, timeout)
        except ValueError:
            # The process manager doesn't know about the process anymore
            # because it is finished
            return True

    def add_done_callback(self, fn):
        if self._terminated.is_set():
            fn(self)
        else:
            self._callbacks.append(fn)

    @protected
    def on_process_finish(self, process):
        self._terminate()

    @protected
    def on_process_fail(self, process):
        self._terminate()

    def _terminate(self):
        self._terminated.set()
        for fn in self._callbacks:
            fn(self)


def wait_for_all(futures):
    for future in futures:
        future.wait()


class ProcessManager(ProcessListener):
    """
    Used to launch processes on multiple threads and monitor their progress
    """

    def __init__(self, max_threads=1024):
        self._processes = {}
        self._executor = ThreadPoolExecutor(max_workers=max_threads)

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
        return self.start(proc_class.new(inputs, pid, logger))

    def start(self, proc):
        """
        Start an existing process.

        :param proc: The process to start
        :type proc: :class:`plum.process.Process`
        :return: A :class:`Future` representing the execution of the process
        :rtype: :class:`Future`
        """
        self._processes[proc.pid] = _ProcInfo(proc, None)
        proc.add_process_listener(self)
        self._play(proc)
        return Future(self, proc)

    def get_processes(self):
        return [info.proc for info in self._processes.values()]

    def play(self, pid):
        try:
            self._play(self._processes[pid].proc)
        except KeyError:
            raise ValueError("Unknown pid")

    def play_all(self):
        for info in self._processes.itervalues():
            self._play(info.proc)

    def pause(self, pid, timeout=None):
        try:
            return self._pause(self._processes[pid].proc, timeout)
        except KeyError:
            raise ValueError("Unknown pid")

    def pause_all(self, timeout=None):
        """
        Pause all processes.  This is a blocking call and will wait until they
        are all paused before returning.
        """
        result = True
        for info in self._processes.values():
            result &= self._pause(info.proc, timeout=timeout)
        return result

    def abort(self, pid, msg=None, timeout=None):
        try:
            return self._abort(self._processes[pid].proc, msg, timeout)
        except KeyError:
            raise ValueError("Unknown pid")

    def abort_all(self, msg=None, timeout=None):
        result = True
        for info in self._processes.values():
            try:
                result &= self._abort(info.proc, msg, timeout)
            except AssertionError:
                # This happens if the process is not playing
                pass
        return result

    def wait_for(self, pid, timeout=None):
        try:
            self._processes[pid].executor_future.result(timeout)
        except KeyError:
            raise ValueError("Unknown pid")
        except concurrent.futures.TimeoutError:
            return False

        return True

    def get_num_processes(self):
        return len(self._processes)

    def shutdown(self):
        self.pause_all()
        for p in self._processes.values():
            self._delete_process(p.proc)
        self._executor.shutdown(True)

    # region From ProcessListener
    @override
    def on_process_stop(self, process):
        super(ProcessManager, self).on_process_stop(process)
        self._delete_process(process)

    @override
    def on_process_fail(self, process):
        super(ProcessManager, self).on_process_fail(process)
        self._delete_process(process)

    # endregion

    def _play(self, proc):
        if not proc.is_playing():
            info = self._processes[proc.pid]
            info.executor_future = self._executor.submit(proc.play)

    def _pause(self, proc, timeout=None):
        if proc.is_playing():
            info = self._processes[proc.pid]
            proc.pause()
            try:
                info.executor_future.result(timeout)
            except concurrent.futures.TimeoutError:
                return False

        return True

    def _abort(self, proc, msg=None, timeout=None):
        info = self._processes[proc.pid]

        info.proc.abort(msg)

        if timeout is not None:
            try:
                info.executor_future.result(timeout)
            except concurrent.futures.TimeoutError:
                return False

        return True

    def _delete_process(self, proc):
        """
        :param proc: :class:`plum.process.Process`
        """
        # Get rid of the info but save the thread so we can join later
        # on shutdown
        proc.remove_process_listener(self)
        del self._processes[proc.pid]
