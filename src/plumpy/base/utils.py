# -*- coding: utf-8 -*-
from typing import Any, Callable

__all__ = ['super_check', 'call_with_super_check']


def super_check(wrapped: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to add a super check to a function to be used with
    call_with_super_check
    """

    def wrapper(self: Any, *args: Any, **kwargs: Any) -> None:
        msg = f"The function '{wrapped.__name__}' was not called through call_with_super_check"
        assert getattr(self, '_called', 0) >= 1, msg
        wrapped(self, *args, **kwargs)
        self._called -= 1

    # Forward wrapped function name to the decorator to show the correct name in the ``call_with_super_check``
    wrapper.__name__ = wrapped.__name__
    return wrapper


def call_with_super_check(wrapped: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    """
    Call a class method checking that all subclasses called super along the way
    """
    self = wrapped.__self__  # type: ignore  # should actually be MethodType, but mypy does not handle this
    call_count = getattr(self, '_called', 0)
    self._called = call_count + 1
    wrapped(*args, **kwargs)
    msg = f"Base '{wrapped.__name__}' was not called from '{self.__class__}'\nHint: Did you forget to call the super?"
    assert self._called == call_count, msg
