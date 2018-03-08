import kiwipy
import collections
from functools import partial
import sys
import tornado.gen

from plumpy.base_process import schedule_task
from . import events

__all__ = ['Future', 'gather', 'chain', 'copy_future', 'InvalidStateError', 'KilledError', 'CallbackTask']

InvalidStateError = kiwipy.InvalidStateError
KilledError = kiwipy.CancelledError

Future = kiwipy.Future


class Task(object):
    """
    A task is an object that has a future meaning that it will eventually
    be carried out producing a result, an exception or being cancelled.
    """

    def future(self):
        pass


class CallbackTask(Task):
    """
    A task that wraps a call to a function or a coroutine.
    """

    def __init__(self, to_call, *args, **kwargs):
        if not callable(to_call):
            raise TypeError("Must be callable, got '{}'".format(to_call))
        super(CallbackTask, self).__init__()
        self._schedule_callback(to_call, *args, **kwargs)
        self._future = Future()

    def future(self):
        return self._future

    def _schedule_callback(self, coro_or_fn, *args, **kwargs):
        loop = events.get_event_loop()
        loop.add_callback(self.do_call, coro_or_fn, *args, **kwargs)

    @tornado.gen.coroutine
    def do_call(self, fn, *args, **kwargs):
        if not self.future().cancelled():
            try:
                if tornado.gen.is_coroutine_function(fn):
                    result = yield fn(*args, **kwargs)
                else:
                    result = fn(*args, **kwargs)
                self.future().set_result(result)
            except Exception:
                self.future().set_exc_info(sys.exc_info())


class TasksList(Task):
    def __init__(self, awaitables):
        if not isinstance(awaitables, collections.Sequence):
            raise ValueError("Must be a sequence")

        self._futures = gather([get_future(ensure_awaitable(awaitable)) for awaitable in awaitables])

    def future(self):
        return self._future


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


def is_awaitable(obj):
    return isinstance(obj, (Future, Task))


class _EnsureAwaitable(object):
    # Converters functions to create awaitables
    _converters = [CallbackTask]

    def __call__(self, obj):
        """
        Returns an awaitable for the corresponding object.  If the object is an awaitable,
        i.e. a `Future` or a `Task`, then it is returned directly.  If it is a function
        or coroutine it will be wrapped in a Task and its execution scheduled.

        :param obj: The awaitable, function or coroutine
        :return: An awaitable
        """
        if is_awaitable(obj):
            return obj
        else:
            for converter in self._converters:
                try:
                    return converter(obj)
                except (ValueError, TypeError):
                    pass

        raise ValueError("Could not create awaitable for '{}'".format(obj))

    def extend(self, make_awaitable):
        self._converters.insert(0, make_awaitable)


ensure_awaitable = _EnsureAwaitable()


def get_future(awaitable):
    """
    Get the future for an awaitable, if the awaitable is a future it is returned,
    if it is a Task then the corresponding future is returned.

    :param awaitable: The awaitable to get the future for
    :return: The future
    :rtype: :class:`Future`
    """

    if isinstance(awaitable, Future):
        return awaitable
    elif isinstance(awaitable, Task):
        return awaitable.future()
    else:
        raise TypeError("'{}' is not awaitable".format(awaitable))


@tornado.gen.coroutine
def wait(awaitable):
    result = yield get_future(awaitable)
    raise tornado.gen.Return(result)
