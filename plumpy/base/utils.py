import contextlib

__all__ = ['super_check', 'call_with_super_check']


def super_check(fn):
    """
    Decorator to add a super check to a function to be used with
    call_with_super_check
    """

    def new_fn(self, *args, **kwargs):
        assert getattr(self, '_called', 0) >= 1, \
            "The function '{}' was not called through " \
            "call_with_super_check".format(fn.__name__)
        fn(self, *args, **kwargs)
        self._called -= 1

    return new_fn


def call_with_super_check(fn, *args, **kwargs):
    """
    Call a class method checking that all subclasses called super along the way
    """
    self = fn.__self__
    call_count = getattr(self, '_called', 0)
    self._called = call_count + 1
    fn(*args, **kwargs)
    assert self._called == call_count, \
        "Base '{}' was not called from '{}'\n" \
        "Hint: Did you forget to call the " \
        "superclass method?".format(fn.__name__, self.__class__)
