import tornado.concurrent
from functools import partial

from .exceptions import CancelledError

__all__ = ['Future', 'gather', 'copy_future', 'InvalidStateError']


class InvalidStateError(BaseException):
    pass


class Future(tornado.concurrent.Future):
    _cancelled = False

    def set_result(self, result):
        if self.done():
            raise InvalidStateError('Future already done')
        super(Future, self).set_result(result)

    def cancel(self):
        if self.done():
            return False

        self._cancelled = True
        # Get the callbacks scheduled
        self._set_done()
        return True

    def cancelled(self):
        return self._cancelled

    def result(self, timeout=None):
        if self.cancelled():
            raise CancelledError

        return super(Future, self).result(timeout)


def copy_future(a, b):
    """ Copy the status of future a to b unless b is already done in
    which case return"""
    if b.done():
        return

    if a.cancelled():
        b.cancel()
    else:
        try:
            b.set_result(a.result())
        except Exception as e:
            b.set_exc_info(a.exc_info())


def chain(a, b):
    """Chain two futures together so that when one completes, so does the other.

    The result (success or failure) of ``a`` will be copied to ``b``, unless
    ``b`` has already been completed or cancelled by the time ``a`` finishes.
    """

    def copy(future):
        copy_future(future, b)

    a.add_done_callback(copy)


def gather(*args):
    return _GatheringFuture(*args)


class _GatheringFuture(Future):
    def __init__(self, *args):
        super(_GatheringFuture, self).__init__()
        self._children = list(args)
        self._nchildren = len(self._children)
        self._nfinished = 0
        self._result = [None] * self._nchildren

        for i, future in enumerate(self._children):
            future.add_done_callback(partial(self._completed, i))

    def cancel(self):
        for child in self._children:
            child.cancel()

    def _completed(self, i, future):
        if self.cancelled():
            return

        if future.cancelled():
            self.cancel()
        else:
            if future.exception() is not None:
                self._result[i] = future.exception()
            else:
                self._result[i] = future.result()

            # Check if we're all done
            self._nfinished += 1
            if self._nfinished == self._nchildren:
                self.set_result(self._result)
