from __future__ import absolute_import
import logging

from .loaders import *
from .communications import *
from .events import *
from .exceptions import *
from .futures import *
from .persistence import *
from .ports import *
from .processes import *
from .process_states import *
from .process_comms import *
from .process_listener import *
from .mixins import *
from .utils import *
from .version import *
from .workchains import *

__all__ = (events.__all__ + exceptions.__all__ + processes.__all__ +
           utils.__all__ + futures.__all__ + mixins.__all__ +
           persistence.__all__ + communications.__all__ + process_comms.__all__ +
           version.__all__, process_listener.__all__ + workchains.__all__ + loaders.__all__ +
           ports.__all__ + process_states.__all__)


# Do this se we don't get the "No handlers could be found..." warnings that will be produced
# if a user of this library doesn't set any handlers. See
# https://docs.python.org/3.1/library/logging.html#library-config
# for more details
class NullHandler(logging.Handler):
    def emit(self, record):
        pass


logging.getLogger("plumpy").addHandler(NullHandler())
