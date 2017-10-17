import apricotpy
import glob
import os
import os.path as path
import portalocker
import portalocker.utils
import shutil
import tempfile
import traceback
import pickle
from plum.process import Process
from plum.persistence._base import LOGGER

LockException = portalocker.exceptions.LockException

_RUNNING_DIRECTORY = path.join(tempfile.gettempdir(), "running")
_FINISHED_DIRECTORY = path.join(_RUNNING_DIRECTORY, "finished")
_FAILED_DIRECTORY = path.join(_RUNNING_DIRECTORY, "failed")


# If portalocker accepts my pull request to have this incorporated into the
# library then this can be removed. https://github.com/WoLpH/portalocker/pull/34
class RLock(portalocker.Lock):
    """
    A reentrant lock, functions in a similar way to threading.RLock in that it
    can be acquired multiple times.  When the corresponding number of release()
    calls are made the lock will finally release the underlying file lock.
    """

    def __init__(
            self, filename, mode='a', timeout=portalocker.utils.DEFAULT_TIMEOUT,
            check_interval=portalocker.utils.DEFAULT_CHECK_INTERVAL, fail_when_locked=False,
            flags=portalocker.utils.LOCK_METHOD):
        super(RLock, self).__init__(filename, mode, timeout, check_interval,
                                    fail_when_locked, flags)
        self._acquire_count = 0

    def acquire(
            self, timeout=None, check_interval=None, fail_when_locked=None):
        if self._acquire_count >= 1:
            fh = self.fh
        else:
            fh = super(RLock, self).acquire(timeout, check_interval,
                                            fail_when_locked)
        self._acquire_count += 1
        return fh

    def release(self):
        if self._acquire_count == 0:
            raise portalocker.LockException(
                "Cannot release more times than acquired")

        if self._acquire_count == 1:
            super(RLock, self).release()
        self._acquire_count -= 1


def _ensure_directory(dir_path):
    if not path.isdir(dir_path):
        os.makedirs(dir_path)


def _create_saved_instance_state(process):
    """
    Create a saved instance state bundle for a process.
    
    :param process: The process
    :type process: :class:`plum.Process`
    :return: The saved state bundle
    :rtype: :class:`apricotpy.Bundle`
    """
    return apricotpy.persistable.Bundle(process)


def _clear(fileobj):
    """
    Clear the contents of an open file.

    :param fileobj: The (open) file object
    """
    fileobj.seek(0)
    fileobj.truncate()


def save(process, save_file):
    _ensure_directory(path.dirname(save_file))
    with RLock(save_file, 'w+b', timeout=0) as lock:
        _save_locked(process, lock)


def _save_locked(process, lock):
    with lock as f:
        checkpoint = _create_saved_instance_state(process)
        with tempfile.NamedTemporaryFile('w+b') as tmp:
            pickle.dump(checkpoint, tmp)
            tmp.seek(0)
            # Now copy everything over
            _clear(f)
            shutil.copyfileobj(tmp, f)
            f.flush()


class Saver(object):
    """
    This object will save the process class every time a state message is received
    until it is asked to stop.
    """

    def __init__(self, loop, process, save_file, finish_directory=None, fail_directory=None):
        # Create a lock for the pickle
        try:
            self._lock = RLock(save_file, 'w+b', timeout=0)
            self._lock.acquire()
        except portalocker.LockException as e:
            e.message = "Unable to lock pickle '{}' someone else must have locked it.\n" + e.message
            raise e

        self._save_file = save_file
        self._loop = loop
        self._loop.messages().add_listener(
            self._process_message, 'process.{}.*'.format(process.uuid))
        self._stopped = False

        self._fail_directory = fail_directory
        self._finish_directory = finish_directory

        self._save(process)

    def stop(self, delete=False, copy_to=None):
        if self._stopped:
            return False

        self._loop.messages().remove_listener(self._process_message)
        self._loop = None

        if copy_to is not None:
            _ensure_directory(copy_to)
            dest = path.join(copy_to, path.basename(self._save_file))
            shutil.copyfile(self._save_file, dest)

        if delete:
            os.remove(self._save_file)

        self._lock.release()
        self._lock = None
        self._stopped = True

        return True

    def get_save_file(self):
        return self._save_file

    def _process_message(self, loop, subject, body, uuid):
        process = loop.get_object(uuid)
        if process.has_finished():
            self.stop(delete=self._finish_directory is not None, copy_to=self._finish_directory)
        elif process.has_failed():
            self.stop(delete=self._fail_directory is not None, copy_to=self._fail_directory)
        else:
            self._save(process)

    def _save(self, process):
        assert not self._stopped, "Cannot save state, stopped"
        try:
            _save_locked(process, self._lock)
        except BaseException as e:
            LOGGER.debug(
                "Failed trying to save pickle for process '{}': "
                    .format(process.uuid, e.message)
            )


class PicklePersistence(apricotpy.LoopObject):
    """
    Class that uses pickles stored in particular directories to persist the
    instance state of Processes.
    """

    @staticmethod
    def pickle_filename(process):
        return "{}.pickle".format(process.uuid)

    @staticmethod
    def load_checkpoint_from_file(filepath):
        with portalocker.Lock(filepath, 'r', timeout=1) as f:
            return pickle.load(f)

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

    def __init__(self, running_directory=_RUNNING_DIRECTORY,
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

        :param running_directory: The base directory to store all pickles in.
        :type running_directory: str
        :param finished_directory: The (relative) subdirectory to put finished
            Process pickles in.  If None they will be deleted when finished.
        :type finished_directory: str
        :param failed_directory: The (relative) subdirectory to put failed
            Process pickles in.  If None they will be deleted on fail.
        :type failed_directory: str
        """
        super(PicklePersistence, self).__init__()

        self._running_directory = running_directory
        self._finished_directory = finished_directory
        self._failed_directory = failed_directory
        self._savers = {}
        self._persisting = False

    def load_checkpoint(self, pid):
        for check_dir in (self._running_directory, self._failed_directory,
                          self._finished_directory):
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
            except (portalocker.LockException, IOError):
                # Don't load locked checkpoints or those with IOErrors
                # these often come if the pickle was deleted since the glob
                pass
            except BaseException:
                LOGGER.warning(
                    "Failed to load checkpoint '{}' (deleting)\n"
                    "{}".format(f, traceback.format_exc()))

                # Deleting
                try:
                    os.remove(f)
                except OSError:
                    pass

        return checkpoints

    def start_persisting(self):
        """      
        Any new processes inserted into the event loop will be persisted at each state transition.
        """
        if self._persisting:
            return

        self.loop().messages().add_listener(
            self._on_object_inserting, 'loop.object.*.inserting')
        self.loop().messages().add_listener(
            self._on_object_removing, 'loop.object.*.removing')

        self._persisting = True

    def stop_persisting(self):
        """
        Stop automatically persisting processes.
        """
        if not self._persisting:
            return

        self.loop().messages().remove_listener(self._on_object_inserting)
        self.loop().messages().remove_listener(self._on_object_removing)
        self._persisting = False

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
        """
        Start saving the state of the given process.
        :param process: The process to persist
        """
        if process.uuid in self._savers:
            # Already persisted
            return

        save_file = self.get_save_file(process)
        self._savers[process.uuid] = Saver(
            self._loop, process, save_file,
            self.finished_directory, self.failed_directory
        )

    def reset_persisted(self, delete=False):
        for saver in self._savers.values():
            saver.stop(delete=delete)

        self._savers.clear()

    def get_save_file(self, process):
        """
        Get the path where the pickle for a process will be stored.

        :param process: The process
        :return: A string to the absolute path of where the pickle is stored.
        :rtype: str
        """
        return path.join(self._running_directory, self.pickle_filename(process))

    def save(self, process):
        save_file = self.get_save_file(process)
        save(process, save_file)

    def _on_object_inserting(self, loop, subject, uuid, sender_id):
        obj = loop.get_object(uuid)
        if isinstance(obj, Process):
            self._savers[obj.uuid] = Saver(loop, obj, self.get_save_file(obj))
            self.persist_process(obj)

    def _on_object_removing(self, loop, subject, uuid, sender_id):
        if uuid in self._savers:
            saver = self._savers.pop(uuid)
            saver.stop()
