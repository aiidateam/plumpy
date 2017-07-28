# -*- coding: utf-8 -*-

import logging
import threading
from abc import ABCMeta, abstractmethod

import apricotpy
from plum.exceptions import Unsupported, Interrupted
from plum.util import fullname, protected, override

_LOGGER = logging.getLogger(__name__)


class WaitOn(apricotpy.PersistableAwaitableLoopObject):
    """
    An object that represents something that is being waited on.
    """

    def __str__(self):
        return self.__class__.__name__
