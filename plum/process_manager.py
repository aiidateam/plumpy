
import threading
from plum.process import ProcessListener
from plum.util import override


class _ProcInfo(object):
    def __init__(self, proc, thread):
        self.proc = proc
        self.thread = thread


class ProcessManager(ProcessListener):
    """
    Used to launch processes on multiple threads and monitor their progress
    """

    def __init__(self):
        self._processes = {}

    def launch(self, proc_class, inputs=None, pid=None, logger=None):
        return self.start(proc_class.new_instance(inputs, pid, logger))

    def start(self, proc):
        self._processes[proc.pid] = _ProcInfo(proc, None)
        self._play(proc)
        return proc.pid

    def play_all(self):
        for info in self._processes.itervalues():
            self._play(info.proc)

    def pause_all(self):
        """
        Pause all processes.  This is a blocking call and will wait until they
        are all paused before returning.
        """
        for info in self._processes.itervalues():
            self._pause(info.proc)

    def abort_all(self, msg=None):
        for info in self._processes.itervalues():
            info.proc.abort(msg)

    def get_num_processes(self):
        return len(self._processes)

    def shutdown(self):
        self.pause_all()
        self._processes = {}

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

    def _pause(self, proc):
        info = self._processes[proc.pid]
        info.proc.pause()
        # Is is paused or finished already?
        if info.thread is not None:
            info.thread.join()
            info.thread = None

    def _delete_process(self, proc):
        info = self._processes.pop(proc.pid)
        if info.thread is not None:
            info.thread.join()