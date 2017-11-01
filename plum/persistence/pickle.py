import os
import re
import errno
import fnmatch
import pickle

from apricotpy.persistable import Bundle
from plum.persistence import Persister
from plum.persistence import PersistedCheckpoint


class PicklePersister(Persister):
    """
    Implementation of the abstract Persister class that stores Process states
    in pickles on a filesystem.
    """
    PICKLE_SUFFIX = 'pickle'

    def __init__(self, pickle_directory):
        """
        Instantiate a PicklePersister object that will persist processes by
        writing their bundles to a pickle in a directory specified by the
        argument 'pickle_directory'

        :param pickle_directory: the full path to the directory where pickles will be written
        """
        super(PicklePersister, self).__init__()

        try:
            self.ensure_pickle_directory(pickle_directory)
        except OSError as exception:
            raise ValueError('failed to create the pickle directory at {}'.format(pickle_directory))

        self._pickle_directory = pickle_directory


    @staticmethod
    def ensure_pickle_directory(dirpath):
        """
        Will attempt to create the directory at dirpath and raise if it fails, except
        if the exception arose because the directory already existed
        """
        try:
            os.makedirs(dirpath)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    @staticmethod
    def parse_filename(filename):
        """
        Parse the filename of a persisted checkpoint to retrieve the process id
        and the optional tag

        :returns: a tuple of process id and optional tag
        :rtype: PersistedCheckpoint
        :raises ValueError: if filename does not conform to expected format
        """
        regex = '^(([^\.]*)\.)?(.*)\.{}$'.format(PICKLE_SUFFIX)
        matches = re.findall(regex, filename)

        if not matches:
            raise ValueError('invalid pickle filename detected, could not determine pid and or tag')

        if matches[0][0]:
            return PersistedCheckpoint(matches[0][1], matches[0][2])
        else:
            return PersistedCheckpoint(matches[0][2], None)

    @staticmethod
    def pickle_filename(pid, tag=None):
        """
        Returns the relative filepath of the pickle for the given process id
        and optional checkpoint tag
        """
        if tag is not None:
            filename = '{}.{}.{}'.format(pid, tag, self.PICKLE_SUFFIX)
        else:
            filename = '{}.{}'.format(pid, self.PICKLE_SUFFIX)

        return filename

    @staticmethod
    def pickle_filepath(pid, tag=None):
        """
        Returns the full filepath of the pickle for the given process id
        and optional checkpoint tag
        """
        return os.path.join(self._pickle_directory, self.pickle_filename(pid, tag))

    def save_checkpoint(self, process, tag=None):
        """
        Persist a process to a pickle on disk

        :param process: :class:`plum.process.Process`
        :param tag: optional checkpoint identifier to allow distinguishing
            multiple checkpoints for the same process
        """
        bundle = Bundle(process)

        with open(self.pickle_filepath(process.pid, tag), 'w+b') as handle:
            pickle.dump(bundle, handle)

    def load_checkpoint(self, pid, tag=None):
        """
        Load a process from a persisted checkpoint by its process id

        :param pid: the process id of the :class:`plum.process.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        :return: a bundle with the process state
        :rtype: :class:`apricotpy.persistable.Bundle`
        """
        try:
            bundle = pickle.load(self.pickle_filepath(pid, tag))
        except IOError as exception:
            raise

        return bundle

    def get_checkpoints(self):
        """
        Return a list of all the current persisted process checkpoints
        with each element containing the process id and optional checkpoint tag

        :return: list of PersistedCheckpoint tuples
        """
        checkpoints = []
        file_pattern = '*.{}'.format(self.PICKLE_SUFFIX)

        for subdir, dirs, files in os.walk(self._pickle_directory):
            for filename in fnmatch.filter(files, file_pattern):
                try:
                    checkpoint = self.parse_filename(filename)
                except ValueError:
                    continue
                else:
                    checkpoints.append(checkpoint)

        return checkpoints

    def get_process_checkpoints(self, pid):
        """
        Return a list of all the current persisted process checkpoints for the
        specified process with each element containing the process id and
        optional checkpoint tag

        :param pid: the process pid
        :return: list of PersistedCheckpoint tuples
        """
        return [checkpoint for c in self.get_checkpoints if c.pid == pid]

    def delete_checkpoint(self, pid, tag=None):
        """
        Delete a persisted process checkpoint. No error will be raised if
        the checkpoint does not exist

        :param pid: the process id of the :class:`plum.process.Process`
        :param tag: optional checkpoint identifier to allow retrieving
            a specific sub checkpoint for the corresponding process
        """
        pickle_filepath = self.pickle_filepath(pid, tag)

        try:
            os.remove(pickle_filepath)
        except OSError:
            pass

    def delete_process_checkpoints(self, pid):
        """
        Delete all persisted checkpoints related to the given process id

        :param pid: the process id of the :class:`plum.process.Process`
        """
        for checkpoint in self.get_process_checkpoints(pid):
            self.delete_checkpoint(checkpoint.pid, checkpoint.tag)