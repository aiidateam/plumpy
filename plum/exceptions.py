class CancelledError(Exception):
    pass


class ClassNotFoundException(ImportError):
    pass


class TimeoutError(Exception):
    pass


class Unsupported(Exception):
    pass


class LockError(Exception):
    pass


class ProcessFailed(Exception):
    pass


class Interrupted(Exception):
    pass


class InvalidStateError(Exception):
    pass
