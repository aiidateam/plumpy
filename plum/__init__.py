from __future__ import absolute_import
from tornado import ioloop
import logging

from .communications import *
from .events import *
from .exceptions import *
from .futures import *
from .persisters import *
from .process import *
from .process_comms import *
from . import stack
from .mixins import *
from .utils import *
from .version import *

__all__ = (events.__all__ + exceptions.__all__ + process.__all__ +
           utils.__all__ + futures.__all__ + mixins.__all__ + ['stack'] +
           persisters.__all__ + communications.__all__ + process_comms.__all__ +
           version.__all__)

PersistableEventLoop = ioloop


# Do this se we don't get the "No handlers could be found..." warnings that will be produced
# if a user of this library doesn't set any handlers. See
# https://docs.python.org/3.1/library/logging.html#library-config
# for more details
class NullHandler(logging.Handler):
    def emit(self, record):
        pass


logging.getLogger("plum").addHandler(NullHandler())
