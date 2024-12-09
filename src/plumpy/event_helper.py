# -*- coding: utf-8 -*-
import logging
from typing import TYPE_CHECKING, Any, Callable, Optional

from plumpy.utils import SAVED_STATE_TYPE

from . import persistence
from plumpy.persistence import Savable, LoadSaveContext, _ensure_object_loader, auto_load

if TYPE_CHECKING:
    from typing import Set, Type
    from .process_listener import ProcessListener

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

    @classmethod
    def recreate_from(cls, saved_state: SAVED_STATE_TYPE, load_context: Optional[LoadSaveContext] = None) -> Savable:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = _ensure_object_loader(load_context, saved_state)
        obj = cls.__new__(cls)
        auto_load(obj, saved_state, load_context)
        return obj

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
            except Exception as exception:
                _LOGGER.error("Listener '%s' produced an exception:\n%s", listener, exception)
