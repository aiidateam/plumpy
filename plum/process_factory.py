
from abc import ABCMeta, abstractmethod


class ProcessFactory(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def create_process(self, process_class, inputs=None):
        pass

    @abstractmethod
    def recreate_process(self, checkpoint):
        pass

    def create_checkpoint(self, process, wait_on):
        raise NotImplementedError()
