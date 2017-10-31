import apricotpy
import logging
from .process import *
from .utils import *

__all__ = (process.__all__ + utils.__all__)

EventLoop = apricotpy.BaseEventLoop
PersistableEventLoop = apricotpy.persistable.BaseEventLoop
Bundle = apricotpy.persistable.Bundle


# Do this se we don't get the "No handlers could be found..." warnings that will be produced
# if a user of this library doesn't set any handlers. See
# https://docs.python.org/3.1/library/logging.html#library-config
# for more details
class NullHandler(logging.Handler):
    def emit(self, record):
        pass


logging.getLogger("plum").addHandler(NullHandler())
