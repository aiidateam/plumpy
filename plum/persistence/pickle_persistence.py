
import glob
import os
import tempfile
import pickle
from plum.process import ProcessListener
from plum.util import override
from plum.persistence.checkpoint import Checkpoint


_STORE_DIRECTORY = os.path.join(tempfile.gettempdir(), "process_records")


class PicklePersistence(ProcessListener):
    def __init__(self, process_factory, directory=_STORE_DIRECTORY):
        self._process_factory = process_factory
        self._directory = directory

    @staticmethod
    def load_all_checkpoints(directory=_STORE_DIRECTORY):
        checkpoints = []
        for f in glob.glob(os.path.join(directory, "*.pickle")):
            checkpoints.append(pickle.load(open(f, 'rb')))
        return checkpoints

    def persist_process(self, process):
        process.add_process_listener(self)

    @override
    def on_process_start(self, process):
        self._ensure_directory()
        self.save(process)

    @override
    def on_process_wait(self, process, wait_on):
        self._ensure_directory()
        self.save(process, wait_on)

    @override
    def on_process_finish(self, process, retval):
        os.remove(self._pickle_filename(process))
        process.remove_process_listener(self)

    def _pickle_filename(self, process):
        return os.path.join(self._directory, "{}.pickle".format(process.pid))

    def _ensure_directory(self):
        if not os.path.isdir(self._directory):
            os.makedirs(self._directory)

    def save(self, process, wait_on=None):
        checkpoint = self._process_factory.create_checkpoint(process, wait_on)
        pickle.dump(checkpoint, open(self._pickle_filename(process), 'wb'))
