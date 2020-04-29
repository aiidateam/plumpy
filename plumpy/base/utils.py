# -*- coding: utf-8 -*-
# pylint: disable=protected-access
__all__ = ['super_check', 'call_with_super_check']


def super_check(wrapped):
    """
    Decorator to add a super check to a function to be used with
    call_with_super_check
    """

    def wrapper(self, *args, **kwargs):
        msg = "The function '{}' was not called through call_with_super_check".format(wrapped.__name__)
        assert getattr(self, '_called', 0) >= 1, msg
        wrapped(self, *args, **kwargs)
        self._called -= 1

    return wrapper


def call_with_super_check(wrapped, *args, **kwargs):
    """
    Call a class method checking that all subclasses called super along the way
    """
    self = wrapped.__self__
    call_count = getattr(self, '_called', 0)
    self._called = call_count + 1
    wrapped(*args, **kwargs)
    msg = "Base '{}' was not called from '{}'\nHint: Did you forget to call the superclass method?".format(
        wrapped.__name__, self.__class__
    )
    assert self._called == call_count, msg
