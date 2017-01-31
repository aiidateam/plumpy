
import threading
from plum.process import ProcessListener
from plum.util import override


class _ProcInfo(object):
    def __init__(self, proc, thread):
        self.proc = proc
        self.thread = thread


class Future(object):
    def __init__(self, procman, process):
        """
        The process manager creates instances of futures that can be used by the
        user.

        :param procman: The process manager that the process belongs to
        :type procman: :class:`ProcessManager`
        :param pid: The pid of the process
        """
        self._procman = procman
        self._process = process

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

    def abort(self, msg=None, timeout=None):
        return self._procman.abort(self.pid, msg, timeout)

    def play(self):
        return self._procman.play(self.pid)

    def pause(self, timeout):
        return self._procman.pause(self.pid, timeout)


class ProcessManager(ProcessListener):
    """
    Used to launch processes on multiple threads and monitor their progress
    """

    def __init__(self):
        self._processes = {}
        self._finished_threads = []

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
        return self.start(proc_class.new_instance(inputs, pid, logger))

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
        return [info.proc for info in self._processes.itervalues()]

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
        for info in self._processes.itervalues():
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
            result &= self._abort(info.proc, msg, timeout)
        return result

    def get_num_processes(self):
        return len(self._processes)

    def shutdown(self):
        self.pause_all()
        self._processes = {}
        for t in self._finished_threads:
            t.join()

    # From ProcessListener ##################################
    @override
    def on_process_stop(self, process):
        super(ProcessManager, self).on_process_stop(process)
        self._delete_process(process)

    @override
    def on_process_fail(self, process):
        super(ProcessManager, self).on_process_fail(process)
        self._delete_process(process)
    #########################################################

    def _play(self, proc):
        info = self._processes[proc.pid]
        # Is it playing already?
        if info.thread is None:
            info.thread = threading.Thread(target=proc.start)
            info.thread.start()

    def _pause(self, proc, timeout=None):
        info = self._processes[proc.pid]
        info.proc.pause()
        # Is is paused or finished already?
        thread = info.thread
        if thread is not None:
            thread.join(timeout)
            info.thread = None
            if thread.is_alive():
                self._finished_threads.append(thread)
                return False
            else:
                return True
        return True

    def _abort(self, proc, msg=None, timeout=None):
        info = self._processes[proc.pid]
        # This will cause a stop message and the process will be deleted
        info.proc.abort(msg)
        if info.thread is not None and timeout != 0.0:
            info.thread.join(timeout)
            return not info.thread.is_alive()
        return True

    def _delete_process(self, proc):
        """
        :param proc: :class:`plum.process.Process`
        """
        # Get rid of the info but save the thread so we can join later
        # on shutdown
        proc.remove_process_listener(self)
        thread = self._processes.pop(proc.pid).thread
        self._finished_threads.append(thread)