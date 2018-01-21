import abc
import kiwi
from future.utils import with_metaclass

from . import futures

__all__ = ['Communicator', 'RemoteException', 'DeliveryFailed', 'TaskRejected']

RemoteException = kiwi.RemoteException
DeliveryFailed = kiwi.DeliveryFailed
TaskRejected = kiwi.TaskRejected
Communicator = kiwi.Communicator


class Action(futures.Future):
    def execute(self, publisher):
        pass
