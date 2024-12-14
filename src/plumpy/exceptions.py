# -*- coding: utf-8 -*-
from typing import Optional

__all__ = [
    'ClosedError',
    'InvalidStateError',
    'KilledError',
    'PersistenceError',
    'UnsuccessfulResult',
]


class KilledError(Exception):
    """The process was killed."""


class InvalidStateError(Exception):
    """Raised when an operation is attempted that requires the process to be in a state
    that is different from the current state
    """


class UnsuccessfulResult:
    """The result of the process was unsuccessful"""

    def __init__(self, result: Optional[int] = None):
        """Initialise.

        :param result: the exit code of the process

        """
        self.result = result


class PersistenceError(Exception):
    """Raised when there is a problem persisting the process"""


class ClosedError(Exception):
    """Raised when an mutable operation is attempted on a closed process"""

class TaskRejectedError(Exception):
    """ A task was rejected by the coordinacor"""
