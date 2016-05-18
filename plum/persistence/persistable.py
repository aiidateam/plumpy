
from abc import ABCMeta, abstractmethod


class Persistable(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def save_instance_state(self, bundle):
        pass
