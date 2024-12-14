from typing import Any, Callable, Protocol

RpcSubscriber = Callable[['Communicator', Any], Any]
BroadcastSubscriber = Callable[['Communicator', Any, Any, Any, Any], Any]

class Communicator(Protocol):

    def add_rpc_subscriber(self, subscriber: RpcSubscriber, identifier=None) -> Any:
        ...

    def add_broadcast_subscriber(self, subscriber: BroadcastSubscriber, identifier=None) -> Any:
        ...

    def remove_rpc_subscriber(self, identifier):
        ...

    def remove_broadcast_subscriber(self, identifier):
        ...

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None) -> bool:
        ...
