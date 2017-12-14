import tornado.concurrent
from functools import partial

from .exceptions import CancelledError

__all__ = ['Future', 'gather']


class Future(tornado.concurrent.Future):
    _cancelled = False

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


def _copy(a, b):
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
        _copy(future, b)

    a.add_done_callback(copy)


def gather(*args):
    return _GatheringFuture(*args)


class _GatheringFuture(Future):
    def __init__(self, *args):
        super(_GatheringFuture, self).__init__()
        self.__total = len(args)
        self.__num_set = 0
        self.__result = [None] * self.__total

        for i in range(self.__total):
            args[i].add_done_callback(partial(self._completed, i))

    def _completed(self, i, future):
        if self.cancelled():
            return

        if future.cancelled():
            self.cancel()
        else:
            if future.exception() is not None:
                self.__result[i] = future.exception()
            else:
                self.__result[i] = future.result()

            # Check if we're all done
            self.__num_set += 1
            if self.__num_set == self.__total:
                self.set_result(self.__result)
