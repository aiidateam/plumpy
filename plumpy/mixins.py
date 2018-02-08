# -*- coding: utf-8 -*-
from .utils import AttributesDict

from . import persistence

__all__ = ['ContextMixin']


class ContextMixin(persistence.Savable):
    """
    Add a context to a Process.  The contents of the context will be saved
    in the instance state unlike standard instance variables.
    """
    CONTEXT = '_context'

    def __init__(self, *args, **kwargs):
        super(ContextMixin, self).__init__(*args, **kwargs)
        self._context = AttributesDict()

    @property
    def ctx(self):
        return self._context

    def save_instance_state(self, out_state):
        super(ContextMixin, self).save_instance_state(out_state)
        if self._context is not None:
            out_state[self.CONTEXT] = self._context.__dict__

    def load_instance_state(self, saved_state, load_context):
        super(ContextMixin, self).load_instance_state(saved_state, load_context)
        try:
            self._context = AttributesDict(**saved_state[self.CONTEXT])
        except KeyError:
            pass
