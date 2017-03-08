import threading
from plum.persistence import Bundle
from plum.process_listener import ProcessListener


class SaveOnTransition(ProcessListener):
    """
    A class to save the process instance state during a state transition message.
    """
    def __init__(self):
        self._bundle = Bundle()
        self._callbacks = []

    def add_save_callback(self, fn):
        self._callbacks.append(fn)

    # region State transitions
    def on_process_start(self, process):
        self.save(process)

    def on_process_run(self, process):
        self.save(process)

    def on_process_wait(self, process):
        self.save(process)

    def on_process_resume(self, process):
        self.save(process)

    def on_process_finish(self, process):
        self.save(process)

    def on_process_stop(self, process):
        self.save(process)

    def on_process_fail(self, process):
        self.save(process)
        # endregion

    def _save(self, process):
        process.save_instance_state(self._bundle)
        for fn in self._callbacks:
            fn(process, self._bundle)


class _SaveAsSoonAsPossible(object):
    def __init__(self, process):
        self._saved = threading.Event()
        self._bundle = None
        self._saver = SaveOnTransition()
        self._saver.add_save_callback(self._prod_saved)

        process.add_process_listener(self._saver)
        if process.has_terminated():
            self._terminate(process)

    def get_saved_state(self, timeout=None):
        self._saved.wait(timeout)
        return self._bundle

    def _prod_saved(self, process, bundle):
        self._bundle = bundle
        self._terminate(process)

    def _terminate(self, process):
        process.remove_process_listener(self._saver)
        self._saved.set()


def save_on_next_transition(process):
    """
    This method will wait until the next transition of the process at which point
    it will save the state and return it as a bundle.

    :param process: The process to wait on
    :return: The saved state
    :rtype: :class:`plum.persistence.Bundle`
    """
    return _SaveAsSoonAsPossible(process).get_saved_state()
