import functools
import reprlib
import inspect

from . import objects
from . import tasks


def _get_function_source(func):
    if hasattr(func, '__wrapped__'):
        func = func.__wrapped__
    if inspect.isfunction(func):
        code = func.__code__
        return code.co_filename, code.co_firstlineno
    if isinstance(func, functools.partial):
        return _get_function_source(func.func)

    return None


def _format_args_and_kwargs(args, kwargs):
    """
    Format function arguments and keyword arguments.
    Special case for a single parameter: ('hello',) is formatted as ('hello').
    """
    # use reprlib to limit the length of the output
    items = []
    if args:
        items.extend(reprlib.repr(arg) for arg in args)
    if kwargs:
        items.extend('{}={}'.format(k, reprlib.repr(v))
                     for k, v in kwargs.items())
    return '(' + ', '.join(items) + ')'


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


def _format_callback_source(func, args):
    func_repr = _format_callback(func, args, None)
    source = _get_function_source(func)
    if source:
        func_repr += ' at %s:%s' % source
    return func_repr


class Handle(object):
    def __init__(self, fn, args, loop):
        self._loop = loop
        self._fn = fn
        self._args = args
        self._cancelled = False
        self._repr = None

    def _repr_info(self):
        info = [self.__class__.__name__]

        if self._cancelled:
            info.append('cancelled')

        if self._fn is not None:
            info.append(_format_callback_source(self._fn, self._args))

        return info

    def __repr__(self):
        if self._repr is not None:
            return self._repr
        info = self._repr_info()
        return '<%s>' % ' '.join(info)

    def cancel(self):
        if self._cancelled:
            return False

        self._cancelled = True
        self._fn = None
        self._args = None

        return True

    def _run(self):
        assert not self._cancelled, "Cannot run a cancelled callback"

        self._fn(*self._args)


class TimerHandle(Handle):
    """
    Handle for callbacks scheduled at a given time
    """

    __slots__ = ['_scheduled', '_when']

    def __init__(self, when, fn, args, loop):
        assert when is not None
        super(TimerHandle, self).__init__(fn, args, loop)
        self._when = when
        self._scheduled = False

    def _repr_info(self):
        info = super(TimerHandle, self)._repr_info()
        pos = 2 if self._cancelled else 1
        info.insert(pos, 'when=%s' % self._when)
        return info

    def __hash__(self):
        return hash(self._when)

    def __lt__(self, other):
        return self._when < other._when

    def __le__(self, other):
        if self._when < other._when:
            return True
        return self.__eq__(other)

    def __gt__(self, other):
        return self._when > other._when

    def __ge__(self, other):
        if self._when > other._when:
            return True
        return self.__eq__(other)

    def __eq__(self, other):
        if isinstance(other, TimerHandle):
            return (self._when == other._when and
                    self._fn == other._fn and
                    self._args == other._args and
                    self._cancelled == other._cancelled)
        return NotImplemented

    def __ne__(self, other):
        equal = self.__eq__(other)
        return NotImplemented if equal is NotImplemented else not equal


class ExecutorHandle(object):
    def __init__(self, fn, args, executor):
        self._fn = fn
        self._args = args
        self._executor = executor

        self._cancelled = False
        self._exec_future = None

    def cancel(self):
        if not self._cancelled:
            self._cancelled = True
            if self._exec_future is not None:
                self._exec_future.cancel()
            self._fn = None
            self._args = None

    def _run(self):
        return self._executor.submit(self._fn, *self._args)


class Timer(objects.Ticking, objects.Awaitable, objects.LoopObject):
    def __init__(self, engine, when, callback, args):
        super(Timer, self).__init__(engine)

        self._when = when
        self._callback = callback
        self._args = args

    def tick(self):
        time = self.engine().time()
        if time >= self._when:
            self.pause()
            self.engine().call_soon(self._callback, *self._args)
            self.set_result(time)
