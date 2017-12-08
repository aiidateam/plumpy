import logging
try:
    # Use builtin asyncio on Python 3.4+, or asyncio on Python 3.3
    import asyncio
except ImportError:
    # Use Trollius on Python <= 3.2
    import trollius as asyncio
from .base import *
from .events import *
from .process import *
from .utils import *
from . import persistence

__all__ = (events.__all__ + process.__all__ + utils.__all__)

Bundle = persistence.Bundle


# Do this se we don't get the "No handlers could be found..." warnings that will be produced
# if a user of this library doesn't set any handlers. See
# https://docs.python.org/3.1/library/logging.html#library-config
# for more details
class NullHandler(logging.Handler):
    def emit(self, record):
        pass


logging.getLogger("plum").addHandler(NullHandler())
