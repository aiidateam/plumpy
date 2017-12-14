from tornado import ioloop
import logging
from .base import *
from .events import *
from .exceptions import *
from .futures import *
from .mixins import *
from . import process
from .utils import *
from . import persistence

__all__ = (events.__all__ + exceptions.__all__ + process.__all__ +
           utils.__all__ + futures.__all__ + base.__all__ + mixins.__all__)

PersistableEventLoop = ioloop
Bundle = persistence.Bundle


# Do this se we don't get the "No handlers could be found..." warnings that will be produced
# if a user of this library doesn't set any handlers. See
# https://docs.python.org/3.1/library/logging.html#library-config
# for more details
class NullHandler(logging.Handler):
    def emit(self, record):
        pass


logging.getLogger("plum").addHandler(NullHandler())
