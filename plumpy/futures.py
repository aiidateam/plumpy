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
            with kiwipy.capture_exceptions(self):
                self.set_result(self._action(*args, **kwargs))
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


def unwrap_kiwi_future(future, communicator):
    """
    Create a kiwi future that represents the final results of a nested series of futures,
    meaning that if the futures provided itself resolves to a future the returned
    future will not resolve to a value until the final chain of futures is not a future
    but a concrete value.  If at any point in the chain a future resolves to an exception
    then the returned future will also resolve to that exception.

    :param future: the future to unwrap
    :type future: :class:`kiwipy.Future`
    :return: the unwrapping future
    :rtype: :class:`kiwipy.Future`
    """
    unwrapping = communicator.create_future()

    def unwrap(fut):
        try:
            result = fut.result()
            if isinstance(result, kiwipy.Future):
                result.add_done_callback(unwrap)
            else:
                unwrapping.set_result(result)
        except Exception as exception:  # pylint: disable=broad-except
            unwrapping.set_exception(exception)

    future.add_done_callback(unwrap)
    return unwrapping
