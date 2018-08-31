import kiwipy
from tornado import concurrent, gen, ioloop

__all__ = ['Future', 'gather', 'chain', 'copy_future', 'CancelledError', 'create_task']

CancelledError = kiwipy.CancelledError
InvalidStateError = kiwipy.InvalidStateError

copy_future = kiwipy.copy_future
chain = kiwipy.chain
gather = lambda *args: gen.multi(args)


class Future(concurrent.Future):
    _cancelled = False

    def result(self, timeout=None):
        if self._cancelled:
            raise CancelledError()

        return super(Future, self).result(timeout)

    def remove_done_callback(self, fn):
        self._callbacks.remove(fn)


class CancellableAction(Future):
    def __init__(self, action, cookie=None):
        super(CancellableAction, self).__init__()
        self._action = action
        self._cookie = cookie

    @property
    def cookie(self):
        """ A cookie that can be used to correlate the actions with something """
        return self._cookie

    def set_result(self, result):
        return super(CancellableAction, self).set_result(result)

    def run(self, *args, **kwargs):
        if self.done():
            raise InvalidStateError("Action has already been ran")

        try:
            self.set_result(self._action(*args, **kwargs))
        except Exception as exc:
            self.set_exception(exc)
        finally:
            self._action = None


def create_task(coro, loop=None):
    """
    Schedule a call to a coroutine in the event loop and wrap the outcome
    in a future.

    :param coro: the coroutine to schedule
    :param loop: the event loop to schedule it in
    :return: the future representing the outcome of the coroutine
    :rtype: :class:`tornado.concurrent.Future`
    """
    if loop is None:
        loop = ioloop.IOLoop.current()

    future = concurrent.Future()

    @gen.coroutine
    def do():
        try:
            future.set_result((yield coro()))
        except Exception as exception:
            future.set_exception(exception)

    loop.add_callback(do)
    return future
