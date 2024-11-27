# -*- coding: utf-8 -*-
"""
Python language utilities and tools.
"""

import functools
import inspect
from typing import Any, Callable


def protected(check: bool = False) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def wrap(func: Callable[..., Any]) -> Callable[..., Any]:
        if isinstance(func, property):
            raise RuntimeError('Protected must go after @property decorator')

        args = inspect.getfullargspec(func)[0]
        if len(args) == 0:
            raise RuntimeError('Can only use the protected decorator on member functions')

        # We can only perform checks if the interpreter is capable of giving
        # us the stack i.e. currentframe() produces a valid object
        if check and inspect.currentframe() is not None:

            @functools.wraps(func)
            def wrapped_fn(self: Any, *args: Any, **kwargs: Any) -> Callable[..., Any]:
                try:
                    calling_class = inspect.stack()[1][0].f_locals['self']
                    assert self is calling_class
                except (KeyError, AssertionError):
                    raise RuntimeError(f'Cannot access protected function {func.__name__} from outside class hierarchy')

                return func(self, *args, **kwargs)
        else:
            wrapped_fn = func

        return wrapped_fn

    return wrap


def override(check: bool = False) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to override a superclass method."""

    def wrap(func: Callable[..., Any]) -> Callable[..., Any]:
        if isinstance(func, property):
            raise RuntimeError('Override must go after @property decorator')

        args = inspect.getfullargspec(func)[0]
        if len(args) == 0:
            raise RuntimeError('Can only use the override decorator on member functions')

        if check:

            @functools.wraps(func)
            def wrapped_fn(self: Any, *args: Any, **kwargs: Any) -> Callable[..., Any]:
                try:
                    getattr(super(self.__class__, self), func.__name__)
                except AttributeError:
                    raise RuntimeError(f'Function {func} does not override a superclass method')

                return func(self, *args, **kwargs)
        else:
            wrapped_fn = func

        return wrapped_fn

    return wrap


class __NULL:  # pylint: disable=invalid-name
    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__)


NULL = __NULL()
