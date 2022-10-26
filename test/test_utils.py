# -*- coding: utf-8 -*-
import asyncio
import functools
import inspect
import warnings

import pytest

from plumpy.utils import AttributesFrozendict, ensure_coroutine, load_function


class TestAttributesFrozendict:

    def test_getitem(self):
        d = AttributesFrozendict({'a': 5})
        assert d['a'] == 5

        with pytest.raises(KeyError):
            d['b']

    def test_getattr(self):
        d = AttributesFrozendict({'a': 5})
        assert d.a == 5

        with pytest.raises(AttributeError):
            d.b

    def test_setitem(self):
        d = AttributesFrozendict()
        with pytest.raises(TypeError):
            d['a'] = 5


def fct():
    pass


async def async_fct():
    pass


class TestEnsureCoroutine:

    def test_sync_func(self):
        coro = ensure_coroutine(fct)
        assert inspect.iscoroutinefunction(coro)

    def test_async_func(self):
        coro = ensure_coroutine(async_fct)
        assert coro is async_fct

    def test_callable_class(self):

        class AsyncDummy:

            async def __call__(self):
                pass

        coro = ensure_coroutine(AsyncDummy)
        assert coro is AsyncDummy

    def test_callable_object(self):

        class AsyncDummy:

            async def __call__(self):
                pass

        obj = AsyncDummy()
        coro = ensure_coroutine(obj)
        assert coro is obj

    def test_functools_partial(self):
        fct_wrap = functools.partial(fct)
        coro = ensure_coroutine(fct_wrap)
        assert coro is not fct_wrap
        # The following will emit a RuntimeWarning ``coroutine 'ensure_coroutine.<locals>.wrap' was never awaited``
        # since were not actually ever awaiting ``core`` but that is not the point of the test.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            assert asyncio.iscoroutine(coro())


def test_load_function():
    func = load_function('plumpy.utils.load_function')
    assert func == load_function
