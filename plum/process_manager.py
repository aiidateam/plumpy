
from abc import ABCMeta, abstractmethod


class ProcessManager(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def create_process(self, process_class, inputs):
        pass

    @abstractmethod
    def destroy_process(self, process):
        pass

    @abstractmethod
    def get_process(self, process):
        pass
