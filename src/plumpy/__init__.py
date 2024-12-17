# -*- coding: utf-8 -*-
# mypy: disable-error-code=name-defined
__version__ = '0.24.0'

import logging

# interfaces
from .controller import ProcessController
from .coordinator import Coordinator
from .events import *
from .exceptions import *
from .futures import *
from .loaders import *
from .message import *
from .mixins import *
from .persistence import *
from .ports import *
from .process_listener import *
from .process_states import *
from .processes import *
from .utils import *
from .workchains import *
from .rmq import *

__all__ = (
    events.__all__
    + exceptions.__all__
    + processes.__all__
    + utils.__all__
    + futures.__all__
    + mixins.__all__
    + persistence.__all__
    + message.__all__
    + process_listener.__all__
    + workchains.__all__
    + loaders.__all__
    + ports.__all__
    + process_states.__all__
) + ['ProcessController', 'Coordinator']


# Do this se we don't get the "No handlers could be found..." warnings that will be produced
# if a user of this library doesn't set any handlers. See
# https://docs.python.org/3.1/library/logging.html#library-config
# for more details
class NullHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        pass


logging.getLogger('plumpy').addHandler(NullHandler())
