
from abc import ABCMeta, abstractmethod


class PersistenceManager(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def create_running_process_record(self, process, inputs, pid):
        pass

    @abstractmethod
    def get_record(self, pid):
        pass

    @abstractmethod
    def delete_record(self, pid):
        pass
