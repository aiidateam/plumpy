import glob
import os
import os.path as path
import tempfile
import pickle
from plum.persistence.bundle import Bundle
from plum.process_listener import ProcessListener
from plum.process_monitor import MONITOR, ProcessMonitorListener
from plum.util import override, protected
from plum.persistence._base import LOGGER

_RUNNING_DIRECTORY = path.join(tempfile.gettempdir(), "running")
_FINISHED_DIRECTORY = path.join(_RUNNING_DIRECTORY, "finished")
_FAILED_DIRECTORY = path.join(_RUNNING_DIRECTORY, "failed")


class PicklePersistence(ProcessListener, ProcessMonitorListener):
    """
    Class that uses pickles stored in particular directories to persist the
    instance state of Processes.
    """

    @staticmethod
    def pickle_filename(pid):
        return "{}.pickle".format(pid)

    @classmethod
    def create_from_basedir(cls, basedir, **kwargs):
        """
        Create using a base directory, the pickles will be stored in:
          - running: [basedir]/running
          - finished: [basedir]/finished
          - failed: [basedir]/failed

        :param basedir: The base directory to storage pickle under
        :type basedir: str
        :param kwargs: Any additional arguments to pass to the constructor
        :return: A new instance.
        :rtype: :class:`PicklePersistence`.
        """
        if kwargs is None:
            kwargs = {}

        # Set up the subdirectories
        kwargs['running_directory'] = path.join(basedir, "running")
        kwargs['finished_directory'] = path.join(basedir, "finished")
        kwargs['failed_directory'] = path.join(basedir, "failed")
        return cls(**kwargs)

    def __init__(self, auto_persist=False,
                 running_directory=_RUNNING_DIRECTORY,
                 finished_directory=_FINISHED_DIRECTORY,
                 failed_directory=_FAILED_DIRECTORY):
        """
        Create the pickle persistence object.  If auto_persist is True then
        this object will automatically persist any Processes that are created
        and will keep their persisted state up to date as they run.  By default
        this is turned off as the user may prefer to manually specify which
        process should be persisted.

        The directory structure that will be used is:

        running_directory/[pid].pickle - Currently active processes
        finished_directory/[pid].pickle - Finished processes
        failed_directory/[pid].pickle - Failed processes

        :param auto_persist: Will automatically persist Processes if True.
        :type auto_persist: bool
        :param running_directory: The base directory to store all pickles in.
        :type running_directory: str
        :param finished_directory: The (relative) subdirectory to put finished
            Process pickles in.  If None they will be deleted when finished.
        :type finished_directory: str
        :param failed_directory: The (relative) subdirectory to put failed
            Process pickles in.  If None they will be deleted on fail.
        :type failed_directory: str
        """
        self._running_directory = running_directory
        self._finished_directory = finished_directory
        self._failed_directory = failed_directory
        self._auto_persist = auto_persist

        MONITOR.start_listening(self)

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
            except BaseException as e:
                LOGGER.warning(
                    "Failed to load checkpoint {} because of exception\n"
                    "{}".format(f, e.message))

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
        if not path.isfile(self.get_running_path(process.pid)):
            try:
                self.save(process)
            except pickle.PicklingError as e:
                LOGGER.error(
                    "exception raised trying to pickle process (pid={}).\n"
                    "{}".format(process.pid, e.message))

        try:
            process.add_process_listener(self)
        except AssertionError:
            # Happens if we're already listening
            pass

    def get_running_path(self, pid):
        """
        Get the path where the pickle for a process with pid will be stored
        while it's running.
        :param pid: The process pid
        :return: A string to the absolute path of where the pickle is stored.
        """
        return path.join(self._running_directory, self.pickle_filename(pid))

    def save(self, process):
        checkpoint = self.create_bundle(process)
        self._ensure_directory(self._running_directory)
        filename = self.get_running_path(process.pid)
        try:
            with open(filename, 'wb') as f:
                pickle.dump(checkpoint, f)
        except pickle.PickleError:
            # Don't leave a half-baked pickle around
            if path.isfile(filename):
                os.remove(filename)
            raise

    # ProcessListener messages #################################################
    @override
    def on_process_run(self, process):
        try:
            self.save(process)
        except pickle.PicklingError:
            LOGGER.error("exception raised trying to pickle process (pid={}) "
                         "during on_run message.".format(process.pid))

    @override
    def on_process_wait(self, process):
        try:
            self.save(process)
        except pickle.PicklingError:
            LOGGER.error("exception raised trying to pickle process (pid={}) "
                         "during on_wait message.".format(process.pid))

    @override
    def on_process_finish(self, process):
        try:
            self.save(process)
            self._release_process(process.pid, self.finished_directory)
        except pickle.PicklingError:
            LOGGER.error("exception raised trying to pickle process (pid={}) "
                         "during on_finish message.".format(process.pid))
        except ValueError:
            pass

    ############################################################################

    # ProcessMonitorListener messages ##########################################
    @override
    def on_monitored_process_failed(self, pid):
        try:
            self._release_process(pid, self.failed_directory)
        except ValueError:
            pass

    ############################################################################

    @override
    def on_monitored_process_registered(self, process):
        if self._auto_persist:
            self.persist_process(process)

    @protected
    def create_bundle(self, process):
        checkpoint = Bundle()
        process.save_instance_state(checkpoint)
        return checkpoint

    @staticmethod
    def _ensure_directory(dir_path):
        if not path.isdir(dir_path):
            os.makedirs(dir_path)

    def _release_process(self, pid, save_path):
        # Get the current location of the pickle
        pickle_path = self.get_running_path(pid)

        if path.isfile(pickle_path):
            if save_path is not None:
                self._ensure_directory(save_path)
                to = path.join(save_path, self.pickle_filename(pid))
                os.rename(pickle_path, to)
            else:
                os.remove(pickle_path)
        else:
            raise ValueError(
                "Cannot find pickle for process with pid '{}'".format(pid))
