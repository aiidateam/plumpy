
import glob
import os
import os.path as path
import tempfile
import pickle
from plum.process import ProcessListener
from plum.util import override
from plum.persistence._base import LOGGER


_STORE_DIRECTORY = path.join(tempfile.gettempdir(), "process_records")
_FINISHED_DIRECTORY = path.join(_STORE_DIRECTORY, "finished")
_FAILED_DIRECTORY = path.join(_STORE_DIRECTORY, "failed")


class PicklePersistence(ProcessListener):
    def __init__(self, process_factory, directory=_STORE_DIRECTORY,
                 finished_directory=_FINISHED_DIRECTORY,
                 failed_directory=_FAILED_DIRECTORY):
        self._process_factory = process_factory
        self._running_directory = directory
        self._finished_directory = finished_directory
        self._failed_directory = failed_directory

    def load_checkpoint(self, pid):
        for check_dir in [self._running_directory, self._failed_directory,
                          self._finished_directory]:
            p = path.join(check_dir, str(pid) + ".pickle")
            if path.isfile(p):
                return self.load_checkpoint_from_file(p)

        raise ValueError(
            "Not checkpoint with pid '{}' could be found".format(pid))

    def load_all_checkpoints(self):
        checkpoints = []
        for f in glob.glob(path.join(self._running_directory, "*.pickle")):
            try:
                checkpoints.append(self.load_checkpoint_from_file(f))
            except EOFError:
                pass
        return checkpoints

    def load_checkpoint_from_file(self, filepath):
        with open(filepath, 'rb') as file:
            return pickle.load(file)

    @property
    def store_directory(self):
        return self._running_directory

    @property
    def failed_directory(self):
        return self._failed_directory

    @property
    def finished_directory(self):
        return self._finished_directory

    def persist_process(self, process):
        # If the process doesn't have a persisted state then persist it now
        if not path.isfile(self._running_path(process)):
            try:
                self.save(process)
            except pickle.PicklingError:
                LOGGER.error(
                    "exception raised trying to pickle process (pid={})."
                    .format(process.pid))

        process.add_process_listener(self)

    @override
    def on_process_start(self, process):
        try:
            self.save(process)
        except pickle.PicklingError:
            LOGGER.error("exception raised trying to pickle process (pid={}) "
                         "during on_start message.".format(process.pid))

    @override
    def on_process_wait(self, process, wait_on):
        try:
            self.save(process, wait_on)
        except pickle.PicklingError:
            LOGGER.error("exception raised trying to pickle process (pid={}) "
                         "during on_wait message.".format(process.pid))

    @override
    def on_process_finish(self, process, retval):
        self._release_process(process, self.finished_directory)

    @override
    def on_process_fail(self, process, exception):
        self._release_process(process, self.failed_directory)

    def _release_process(self, process, save_path):
        process.remove_process_listener(self)

        pickle_path = self._running_path(process)
        if path.isfile(pickle_path):
            if save_path is not None:
                self._ensure_directory(save_path)
                to = path.join(save_path, self._pickle_filename(process))
                os.rename(pickle_path, to)
            else:
                os.remove(pickle_path)

    def _pickle_filename(self, process):
        return "{}.pickle".format(process.pid)

    def _running_path(self, process):
        return path.join(self._running_directory, self._pickle_filename(process))

    def _ensure_directory(self, dir_path):
        if not path.isdir(dir_path):
            os.makedirs(dir_path)

    def save(self, process, wait_on=None):
        checkpoint = self._process_factory.create_checkpoint(process, wait_on)
        self._ensure_directory(self._running_directory)
        filename = self._running_path(process)
        try:
            with open(filename, 'wb') as f:
                pickle.dump(checkpoint, f)
        except pickle.PickleError:
            # Don't leave a half-baked pickle around
            if path.isfile(filename):
                os.remove(filename)
            raise
