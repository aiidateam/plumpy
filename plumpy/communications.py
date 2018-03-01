import kiwipy

from . import futures

__all__ = ['Communicator', 'RemoteException', 'DeliveryFailed', 'TaskRejected']

RemoteException = kiwipy.RemoteException
DeliveryFailed = kiwipy.DeliveryFailed
TaskRejected = kiwipy.TaskRejected
Communicator = kiwipy.Communicator


class Action(futures.Future):
    def execute(self, publisher):
        pass
