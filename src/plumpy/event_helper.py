# -*- coding: utf-8 -*-
import logging
from typing import TYPE_CHECKING, Any, Callable

from . import persistence

if TYPE_CHECKING:
    from .process_listener import ProcessListener  # pylint: disable=cyclic-import

_LOGGER = logging.getLogger(__name__)


@persistence.auto_persist('_listeners', '_listener_type')
class EventHelper(persistence.Savable):

    def __init__(self, listener_type: 'Type[ProcessListener]'):
        assert listener_type is not None, 'Must provide valid listener type'

        self._listener_type = listener_type
        self._listeners: 'Set[ProcessListener]' = set()

    def add_listener(self, listener: 'ProcessListener') -> None:
        assert isinstance(listener, self._listener_type), 'Listener is not of right type'
        self._listeners.add(listener)

    def remove_listener(self, listener: 'ProcessListener') -> None:
        self._listeners.discard(listener)

    def remove_all_listeners(self) -> None:
        self._listeners.clear()

    @property
    def listeners(self) -> 'Set[ProcessListener]':
        return self._listeners

    def fire_event(self, event_function: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        """Call an event method on all listeners.

        :param event_function: the method of the ProcessListener
        :param args: arguments to pass to the method
        :param kwargs: keyword arguments to pass to the method

        """
        if event_function is None:
            raise ValueError('Must provide valid event method')

        # Make a copy of the list for iteration just in case it changes in a callback
        for listener in list(self.listeners):
            try:
                getattr(listener, event_function.__name__)(*args, **kwargs)
            except Exception as exception:  # pylint: disable=broad-except
                _LOGGER.error("Listener '%s' produced an exception:\n%s", listener, exception)
