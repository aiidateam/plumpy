"""
Python language utilities and tools.
"""

from __future__ import absolute_import
import functools
from inspect import stack, currentframe
from six import PY2

if PY2:
    from inspect import getargspec as get_arg_spec
else:
    from inspect import getfullargspec as get_arg_spec


def protected(check=False):

    def wrap(func):
        if isinstance(func, property):
            raise RuntimeError("Protected must go after @property decorator")

        args = get_arg_spec(func)[0]  # pylint: disable=deprecated-method
        if len(args) == 0:
            raise RuntimeError("Can only use the protected decorator on member functions")

        # We can only perform checks if the interpreter is capable of giving
        # us the stack i.e. currentframe() produces a valid object
        if check and currentframe() is not None:

            @functools.wraps(func)
            def wrapped_fn(self, *args, **kwargs):
                try:
                    calling_class = stack()[1][0].f_locals['self']
                    assert self is calling_class
                except (KeyError, AssertionError):
                    raise RuntimeError("Cannot access protected function {} from outside"
                                       " class hierarchy".format(func.__name__))

                return func(self, *args, **kwargs)
        else:
            wrapped_fn = func

        return wrapped_fn

    return wrap


def override(check=False):

    def wrap(func):
        if isinstance(func, property):
            raise RuntimeError("Override must go after @property decorator")

        args = get_arg_spec(func)[0]  # pylint: disable=deprecated-method
        if len(args) == 0:
            raise RuntimeError("Can only use the override decorator on member functions")

        if check:

            @functools.wraps(func)
            def wrapped_fn(self, *args, **kwargs):
                try:
                    getattr(super(self.__class__, self), func.__name__)
                except AttributeError:
                    raise RuntimeError("Function {} does not override a superclass method".format(func))

                return func(self, *args, **kwargs)
        else:
            wrapped_fn = func

        return wrapped_fn

    return wrap


class __NULL(object):

    def __eq__(self, other):
        return isinstance(other, self.__class__)


NULL = __NULL()
