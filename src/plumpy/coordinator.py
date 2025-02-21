# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Hashable, Protocol
from re import Pattern

if TYPE_CHECKING:
    ID_TYPE = Hashable
    Receiver = Callable[..., Any]


class Coordinator(Protocol):
    def hook_rpc_receiver(
        self,
        receiver: 'Receiver',
        identifier: 'ID_TYPE | None' = None,
    ) -> Any: ...

    def hook_broadcast_receiver(
        self,
        receiver: 'Receiver',
        subject_filters: list[Hashable | Pattern[str]] | None = None,
        sender_filters: list[Hashable | Pattern[str]] | None = None,
        identifier: 'ID_TYPE | None' = None,
    ) -> Any: ...

    def hook_task_receiver(
        self,
        receiver: 'Receiver',
        identifier: 'ID_TYPE | None' = None,
    ) -> 'ID_TYPE': ...

    def unhook_rpc_receiver(self, identifier: 'ID_TYPE | None') -> None: ...

    def unhook_broadcast_receiver(self, identifier: 'ID_TYPE | None') -> None: ...

    def unhook_task_receiver(self, identifier: 'ID_TYPE') -> None: ...

    def rpc_send(self, recipient_id: Hashable, msg: Any,) -> Any: ...

    def broadcast_send(
        self,
        body: Any | None,
        sender: 'ID_TYPE | None' = None,
        subject: str | None = None,
        correlation_id: 'ID_TYPE | None' = None,
    ) -> Any: ...

    def task_send(self, task: Any, no_reply: bool = False) -> Any: ...

    def close(self) -> None: ...
