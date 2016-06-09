
import glob
import os
import tempfile
import pickle
from plum.process import ProcessListener
from plum.util import override
from plum.persistence.checkpoint import Checkpoint


_STORE_DIRECTORY = os.path.join(tempfile.gettempdir(), "process_records")


class PicklePersistence(ProcessListener):
    @staticmethod
    def load_all_checkpoints():
        checkpoints = []
        for f in glob.glob(os.path.join(_STORE_DIRECTORY, "*.pickle")):
            checkpoints.append(pickle.load(open(f, 'rb')))
        return checkpoints

    def persist_process(self, process):
        process.add_process_listener(self)

    @override
    def on_process_start(self, process, inputs):
        self._ensure_directory()
        self._save(process)

    @override
    def on_process_wait(self, process, wait_on):
        self._ensure_directory()
        self._save(process, wait_on)

    @override
    def on_process_finish(self, process, retval):
        os.remove(self._pickle_filename(process))
        process.remove_process_listener(self)

    @staticmethod
    def _pickle_filename(process):
        return os.path.join(_STORE_DIRECTORY, "{}.pickle".format(process.pid))

    @staticmethod
    def _ensure_directory():
        if not os.path.isdir(_STORE_DIRECTORY):
            os.makedirs(_STORE_DIRECTORY)

    def _save(self, process, wait_on=None):
        checkpoint = Checkpoint(process, wait_on)
        pickle.dump(checkpoint, open(self._pickle_filename(process), 'wb'))
