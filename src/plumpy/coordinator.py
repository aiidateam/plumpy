# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Callable, Hashable, Protocol

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
    # XXX: naming - 'add_message_handler'
    def add_rpc_subscriber(self, subscriber: 'RpcSubscriber', identifier: 'ID_TYPE | None' = None) -> Any: ...

    # XXX: naming - 'add_broadcast_handler'
    def add_broadcast_subscriber(
        self,
        subscriber: 'BroadcastSubscriber',
        subject_filters: list[Hashable | re.Pattern[str]] | None = None,
        sender_filters: list[Hashable | re.Pattern[str]] | None = None,
        identifier: 'ID_TYPE | None' = None,
    ) -> Any: ...

    # XXX: naming - absorbed into 'add_message_handler'
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

    def close(self) -> None: ...


class BroadcastFilter:
    """A filter that can be used to limit the subjects and/or senders that will be received"""

    def __init__(self, subscriber, subject=None, sender=None):  # type: ignore
        self._subscriber = subscriber
        self._subject_filters = []
        self._sender_filters = []
        if subject is not None:
            self.add_subject_filter(subject)
        if sender is not None:
            self.add_sender_filter(sender)

    @property
    def __name__(self):  # type: ignore
        return 'BroadcastFilter'

    def __call__(self, communicator, body, sender=None, subject=None, correlation_id=None):  # type: ignore
        if self.is_filtered(sender, subject):
            return None
        return self._subscriber(communicator, body, sender, subject, correlation_id)

    def is_filtered(self, sender, subject) -> bool:  # type: ignore
        if subject is not None and self._subject_filters and not any(check(subject) for check in self._subject_filters):
            return True

        if sender is not None and self._sender_filters and not any(check(sender) for check in self._sender_filters):
            return True

        return False

    def add_subject_filter(self, subject_filter: re.Pattern[str] | None) -> None:
        self._subject_filters.append(self._ensure_filter(subject_filter))  # type: ignore

    def add_sender_filter(self, sender_filter: re.Pattern[str]) -> None:
        self._sender_filters.append(self._ensure_filter(sender_filter))  # type: ignore

    @classmethod
    def _ensure_filter(cls, filter_value):  # type: ignore
        if isinstance(filter_value, str):
            return re.compile(filter_value.replace('.', '[.]').replace('*', '.*')).match
        if isinstance(filter_value, re.Pattern):  # pylint: disable=isinstance-second-argument-not-valid-type
            return filter_value.match

        return lambda val: val == filter_value

    @classmethod
    def _make_regex(cls, filter_str):  # type: ignore
        """
        :param filter_str: The filter string
        :type filter_str: str
        :return: The regular expression object
        """
        return re.compile(filter_str.replace('.', '[.]'))
