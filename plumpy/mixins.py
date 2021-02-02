# -*- coding: utf-8 -*-
from typing import Any

from .utils import AttributesDict, Optional

from . import persistence
from .utils import SAVED_STATE_TYPE

__all__ = ['ContextMixin']


class ContextMixin(persistence.Savable):
    """
    Add a context to a Process.  The contents of the context will be saved
    in the instance state unlike standard instance variables.
    """
    CONTEXT: str = '_context'

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)  # type: ignore
        self._context: Optional[AttributesDict] = AttributesDict()

    @property
    def ctx(self) -> Optional[AttributesDict]:
        return self._context

    def save_instance_state(
        self, out_state: SAVED_STATE_TYPE, save_context: Optional[persistence.LoadSaveContext]
    ) -> None:
        """Add the instance state to ``out_state``.
        .. important::

            The instance state will contain a pointer to the ``ctx``,
            and so should be deep copied or serialised before persisting.
        """
        super().save_instance_state(out_state, save_context)
        if self._context is not None:
            out_state[self.CONTEXT] = self._context.__dict__

    def load_instance_state(self, saved_state: SAVED_STATE_TYPE, load_context: persistence.LoadSaveContext) -> None:
        super().load_instance_state(saved_state, load_context)
        try:
            self._context = AttributesDict(**saved_state[self.CONTEXT])
        except KeyError:
            pass
