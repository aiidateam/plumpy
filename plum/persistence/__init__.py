from abc import abstractmethod, abstractproperty, ABCMeta
import apricotpy


class Persister(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def save_checkpoint(self, process):
        """
        Persist a Process instance

        :param process: :class:`plum.process.Process`
        """
        pass

    @abstractmethod
    def load_checkpoint(self, pid):
        """
        Load a process from a persisted checkpoint by its process id

        :param pid: the process id of the :class:`plum.process.Process`
        :return: a bundle with the process state
        :rtype: :class:`apricotpy.Bundle`
        """
        pass