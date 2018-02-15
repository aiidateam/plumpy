__all__ = ['KilledError']


class KilledError(Exception):
    pass


class ClassNotFoundException(Exception):
    pass


class TimeoutError(Exception):
    pass


class Unsupported(Exception):
    pass


class LockError(Exception):
    pass


class ProcessExcepted(Exception):
    pass


class Interrupted(Exception):
    pass


class InvalidStateError(Exception):
    pass
