# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Hashable, Pattern, Protocol

if TYPE_CHECKING:
    # identifiers for subscribers
    ID_TYPE = Hashable
    Subscriber = Callable[..., Any]
    # RPC subscriber params: communicator, msg
    RpcSubscriber = Callable[['Coordinator', Any], Any]
    # Task subscriber params: communicator, task
    TaskSubscriber = Callable[['Coordinator', Any], Any]
    # Broadcast subscribers params: communicator, body, sender, subject, correlation id
    BroadcastSubscriber = Callable[['Coordinator', Any, Any, Any, ID_TYPE], Any]


class Coordinator(Protocol):
    def add_rpc_subscriber(self, subscriber: 'RpcSubscriber', identifier: 'ID_TYPE | None' = None) -> Any: ...

    def add_broadcast_subscriber(
        self,
        subscriber: 'BroadcastSubscriber',
        subject_filter: str | Pattern[str] | None = None,
        identifier: 'ID_TYPE | None' = None,
    ) -> Any: ...

    def add_task_subscriber(self, subscriber: 'TaskSubscriber', identifier: 'ID_TYPE | None' = None) -> 'ID_TYPE': ...

    def remove_rpc_subscriber(self, identifier: 'ID_TYPE | None') -> None: ...

    def remove_broadcast_subscriber(self, identifier: 'ID_TYPE | None') -> None: ...

    def remove_task_subscriber(self, identifier: 'ID_TYPE') -> None: ...

    def rpc_send(self, recipient_id: Hashable, msg: Any) -> Any: ...

    def broadcast_send(
        self,
        body: Any | None,
        sender: 'ID_TYPE | None' = None,
        subject: str | None = None,
        correlation_id: 'ID_TYPE | None' = None,
    ) -> Any: ...

    def task_send(self, task: Any, no_reply: bool = False) -> Any: ...
