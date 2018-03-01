import kiwipy
from functools import partial
import sys
import tornado.gen

from . import events

__all__ = ['Future', 'gather', 'chain', 'copy_future', 'InvalidStateError', 'KilledError', 'Task']

InvalidStateError = kiwipy.InvalidStateError
KilledError = kiwipy.CancelledError

Future = kiwipy.Future


class Task(Future):
    def __init__(self, coro_or_fn, *args, **kwargs):
        super(Task, self).__init__()
        self._schedule_callback(coro_or_fn, *args, **kwargs)

    def _schedule_callback(self, coro_or_fn, *args, **kwargs):
        loop = events.get_event_loop()
        loop.add_callback(self.do_call, coro_or_fn, *args, **kwargs)

    @tornado.gen.coroutine
    def do_call(self, fn, *args, **kwargs):
        if not self.cancelled():
            try:
                if tornado.gen.is_coroutine_function(fn):
                    result = yield fn(*args, **kwargs)
                else:
                    result = fn(*args, **kwargs)
                self.set_result(result)
            except BaseException:
                self.set_exc_info(sys.exc_info())


def copy_future(source, target):
    """ Copy the status of future a to b unless b is already done in
    which case return

    :param source: The source future
    :type source: :class:`Future`
    :param target: The target future
    :type target: :class:`Future`
    """

    if target.done():
        return

    if source.cancelled():
        target.cancel()
    else:
        if source.exc_info() is not None:
            target.set_exc_info(source.exc_info())
        else:
            target.set_result(source.result())


def chain(a, b):
    """Chain two futures together so that when one completes, so does the other.

    The result (success or failure) of ``a`` will be copied to ``b``, unless
    ``b`` has already been completed or killed by the time ``a`` finishes.
    """

    a.add_done_callback(lambda first: copy_future(first, b))


def gather(*args):
    if not args:
        future = Future()
        future.set_result([])
        return future
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

    def kill(self):
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
