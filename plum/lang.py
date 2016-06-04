"""
Python language utilities and tools.
"""

import functools
import inspect


def protected(check=True):
    def wrap(func):
        if isinstance(func, property):
            raise RuntimeError("Protected must go after @property decorator")

        args = inspect.getargspec(func)[0]
        if len(args) == 0:
            raise RuntimeError(
                "Can only use the protected decorator on member functions")

        # We can only perform checks if the interpreter is capable of giving
        # us the stack i.e. currentframe() produces a valid object
        if check and inspect.currentframe() is not None:
            @functools.wraps(func)
            def wrapped_fn(self, *args, **kwargs):
                try:
                    calling_class = inspect.stack()[1][0].f_locals['self']
                    assert self is calling_class
                except (KeyError, AssertionError):
                    raise RuntimeError(
                        "Cannot access protected function from outside class hierarchy")

                return func(self, *args, **kwargs)
        else:
            wrapped_fn = func

        return wrapped_fn

    return wrap
