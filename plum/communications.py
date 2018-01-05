import abc
from future.utils import with_metaclass

__all__ = ['Communicator', 'RemoteException', 'Receiver']


class RemoteException(BaseException):
    pass


class Communicator(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def register_receiver(self, receiver, identifier=None):
        pass

    @abc.abstractmethod
    def rpc_send(self, recipient_id, msg):
        """
        Initiate a remote procedure call on a recipient

        :param recipient_id: The recipient identifier
        :param msg: The body of the message
        :return: A future corresponding to the outcome of the call
        """
        pass

    @abc.abstractmethod
    def broadcast_msg(self, msg, reply_to=None, correlation_id=None):
        pass


class Receiver(object):
    def on_rpc_receive(self, msg):
        """
        Receive a remote procedure call sent directly to this receiver.
        :param msg: The RPC message
        :return: The return value will be returned to the sender
        """
        pass

    def on_broadcast_receive(self, msg):
        """
        Receive a broadcast message.
        :param msg: The broadcast message
        :return:
        """
        pass
