import kiwipy
import collections
from functools import partial
from past.builtins import basestring
import sys
import tornado.gen

from . import events

__all__ = ['Future', 'gather', 'chain', 'copy_future', 'InvalidStateError', 'KilledError', 'CallbackTask',
           'ensure_awaitable']

InvalidStateError = kiwipy.InvalidStateError
KilledError = kiwipy.CancelledError

Future = kiwipy.Future


class CallbackTask(object):
    """
    A task that wraps a call to a function or a coroutine.
    """

    def __init__(self, to_call, *args, **kwargs):
        if not callable(to_call):
            raise TypeError("Must be callable, got '{}'".format(to_call))
        super(CallbackTask, self).__init__()
        self._schedule_callback(to_call, *args, **kwargs)
        self._future = Future()

    def __await__(self):
        return self._future.__await__()

    def _schedule_callback(self, coro_or_fn, *args, **kwargs):
        loop = events.get_event_loop()
        loop.add_callback(self.do_call, coro_or_fn, *args, **kwargs)

    @tornado.gen.coroutine
    def do_call(self, fn, *args, **kwargs):
        if not self._future.cancelled():
            try:
                if tornado.gen.is_coroutine_function(fn):
                    result = yield fn(*args, **kwargs)
                else:
                    result = fn(*args, **kwargs)
                self._future.set_result(result)
            except Exception:
                self._future.set_exc_info(sys.exc_info())


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
    if hasattr(obj, '__await__'):
        return True
    elif isinstance(obj, collections.Sequence) and not isinstance(obj, basestring):
        return all([is_awaitable(entry) for entry in obj])
    elif isinstance(obj, collections.Mapping):
        return all([is_awaitable(entry) for entry in obj.values()])

    return False


def _ensure_awaitable_list(awaitables):
    if not isinstance(awaitables, collections.Sequence):
        raise TypeError("Not a sequence of awaitables")
    return [ensure_awaitable(awaitable) for awaitable in awaitables]


def _ensure_awaitable_dict(awaitables):
    if not isinstance(awaitables, collections.Mapping):
        raise TypeError("Not a mapping of awaitables")
    return {key: ensure_awaitable(awaitable) for key, awaitable in awaitables.items()}


class _EnsureAwaitable(object):
    # Converters functions to create awaitables
    _converters = [CallbackTask, _ensure_awaitable_list, _ensure_awaitable_dict]

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
