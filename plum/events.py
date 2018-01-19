import functools
import inspect
import reprlib
import sys
from tornado import ioloop

__all__ = ['new_event_loop', 'set_event_loop', 'get_event_loop', 'run_until_complete']

get_event_loop = ioloop.IOLoop.current
new_event_loop = ioloop.IOLoop


def set_event_loop(loop):
    if loop is None:
        ioloop.IOLoop.clear_instance()
    else:
        loop.make_current()


def run_until_complete(future, loop=None):
    if loop is None:
        loop = get_event_loop()

    def _stop(future):
        loop.stop()

    future.add_done_callback(_stop)
    loop.start()
    return future.result()


def _get_function_source(func):
    func = inspect.unwrap(func)
    if inspect.isfunction(func):
        code = func.__code__
        return (code.co_filename, code.co_firstlineno)
    if isinstance(func, functools.partial):
        return _get_function_source(func.func)
    if isinstance(func, functools.partialmethod):
        return _get_function_source(func.func)
    return None


def _format_callback_source(func, args, kwargs):
    func_repr = _format_callback(func, args, kwargs)
    source = _get_function_source(func)
    if source:
        func_repr += ' at {source[0]}:{source[1]}'
    return func_repr


def _format_args_and_kwargs(args, kwargs):
    """Format function arguments and keyword arguments.
    Special case for a single parameter: ('hello',) is formatted as ('hello').
    """
    # use reprlib to limit the length of the output
    items = []
    if args:
        items.extend(reprlib.repr(arg) for arg in args)
    if kwargs:
        items.extend('{k}={reprlib.repr(v)}' for k, v in kwargs.items())
    return '({})'.format(', '.join(items))


def _format_callback(func, args, kwargs, suffix=''):
    if isinstance(func, functools.partial):
        suffix = _format_args_and_kwargs(args, kwargs) + suffix
        return _format_callback(func.func, func.args, func.keywords, suffix)

    if hasattr(func, '__qualname__'):
        func_repr = getattr(func, '__qualname__')
    elif hasattr(func, '__name__'):
        func_repr = getattr(func, '__name__')
    else:
        func_repr = repr(func)

    func_repr += _format_args_and_kwargs(args, kwargs)
    if suffix:
        func_repr += suffix
    return func_repr


class Handle(object):
    """Object returned by callback registration methods."""

    __slots__ = ('_callback', '_args', '_kwargs', '_process',
                 '_cancelled', '_repr', '__weakref__')

    def __init__(self, process, callback, args, kwargs):
        self._process = process
        self._callback = callback
        self._args = args
        self._kwargs = kwargs
        self._cancelled = False
        self._repr = None

    def _repr_info(self):
        info = [self.__class__.__name__]
        if self._cancelled:
            info.append('cancelled')
        if self._callback is not None:
            info.append(_format_callback_source(
                self._callback, self._args, self._kwargs))
        return info

    def __repr__(self):
        if self._repr is not None:
            return self._repr
        info = self._repr_info()
        return '<{}>'.format(' '.join(info))

    def cancel(self):
        if not self._cancelled:
            self._cancelled = True
            self._callback = None
            self._args = None

    def cancelled(self):
        return self._cancelled

    def _run(self):
        if not self._cancelled:
            try:
                self._callback(*self._args, **self._kwargs)
            except BaseException:
                exc_info = sys.exc_info()
                self._process.callback_failed(self._callback, exc_info[1], exc_info[2])
