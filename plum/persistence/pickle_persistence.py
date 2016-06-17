
import glob
import os
import os.path as path
import tempfile
import pickle
from plum.process import ProcessListener
from plum.util import override
from plum.persistence.checkpoint import Checkpoint


_STORE_DIRECTORY = path.join(tempfile.gettempdir(), "process_records")


class PicklePersistence(ProcessListener):
    def __init__(self, process_factory, directory=_STORE_DIRECTORY):
        self._process_factory = process_factory
        self._directory = directory

    def load_checkpoint(self, pid):
        p = path.join(self._directory, str(pid), ".pickle")
        if not path.isfile(p):
            raise ValueError("No checkpoint found at '{}'".format(p))

        return self.load_checkpoint_from_file(p)

    def load_all_checkpoints(self):
        checkpoints = []
        for f in glob.glob(path.join(self._directory, "*.pickle")):
            checkpoints.append(self.load_checkpoint_from_file(f))
        return checkpoints

    def load_checkpoint_from_file(self, filepath):
        with open(filepath, 'rb') as file:
            return pickle.load(file)

    @property
    def store_directory(self):
        return self._directory

    def persist_process(self, process):
        process.add_process_listener(self)

    @override
    def on_process_start(self, process):
        self._ensure_directory()
        try:
            self.save(process)
        except pickle.PicklingError:
            # TODO: We should emit an error here
            pass

    @override
    def on_process_wait(self, process, wait_on):
        self._ensure_directory()
        try:
            self.save(process, wait_on)
        except pickle.PicklingError:
            # TODO: We should emit an error here
            pass

    @override
    def on_process_finish(self, process, retval):
        os.remove(self._pickle_filename(process))
        process.remove_process_listener(self)

    def _pickle_filename(self, process):
        return path.join( self._directory, "{}.pickle".format(process.pid))

    def _ensure_directory(self):
        if not path.isdir(self._directory):
            os.makedirs(self._directory)

    def save(self, process, wait_on=None):
        checkpoint = self._process_factory.create_checkpoint(process, wait_on)
        self._ensure_directory()
        with open(self._pickle_filename(process), 'wb') as f:
            pickle.dump(checkpoint, f)
