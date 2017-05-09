import time
from plum.process_manager import ProcessManager
from plum.process_listener import ProcessListener


class ProcessController(ProcessListener):
    def __init__(self, executor=None, remove_on_terminate=True):
        """
        :param executor: The executor used to play processes
        :param remove_on_terminate: Automatically remove any process that terminates
            from the control of this class.
        """
        if executor is None:
            self._executor = ProcessManager()
        else:
            self._executor = executor

        self._processes = {}
        self._remove_on_terminate = remove_on_terminate

    def insert(self, process):
        """
        Insert a process into the controller
        
        :param process: The process
        :type process: :class:`plum.process.Process`
        """
        self._processes[process.pid] = process
        process.add_process_listener(self)

    def insert_and_play(self, process):
        self.insert(process)
        self.play(process.pid)

    def remove(self, pid, timeout=None):
        try:
            if not self._executor.pause(pid, timeout):
                return False
        except ValueError:
            pass
        self._processes[pid].remove_process_listener(self)
        del self._processes[pid]
        return True

    def remove_all(self, timeout=None):
        num_removed = 0

        time_left = timeout
        t0 = time.time()
        for pid in self._processes.keys():
            if not self.remove(pid, time_left):
                return num_removed

            num_removed += 1

            if time_left is not None:
                time_left = timeout - (time.time() - t0)

        return num_removed

    def play(self, pid):
        self._executor.play(self._processes[pid])

    def pause(self, pid, timeout=None):
        try:
            return self._executor.pause(pid, timeout)
        except ValueError:
            return False

    def pause_all(self, timeout=None):
        return self._executor.pause_all(timeout)

    def abort(self, pid, message=None, timeout=None):
        try:
            return self._executor.abort(pid, message, timeout)
        except ValueError:
            return False

    def abort_all(self, msg=None, timeout=None):
        return self._executor.abort_all(msg, timeout)

    def on_process_done_playing(self, process):
        if self._remove_on_terminate and process.has_terminated():
            self.remove(process.pid)

    def get_process(self, pid):
        return self._processes[pid]

    def get_processes(self):
        return self._processes.values()

    def get_num_processes(self):
        return len(self._processes)
