__all__ = ['KilledError', 'UnsuccessfulResult', 'InvalidStateError', 'PersistenceError']


class KilledError(Exception):
    """The process was killed."""


class TimeoutError(Exception):
    pass


class Unsupported(Exception):
    pass


class LockError(Exception):
    pass


class ProcessExcepted(Exception):
    pass


class InvalidStateError(Exception):
    pass


class UnsuccessfulResult(object):
    """The result of the process was unsuccessful"""

    def __init__(self, result=None):
        self.result = result


class PersistenceError(Exception):
    pass
