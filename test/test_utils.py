# -*- coding: utf-8 -*-
import unittest
import inspect
import functools

from plumpy.utils import AttributesFrozendict, ensure_coroutine


class TestAttributesFrozendict(unittest.TestCase):

    def test_getitem(self):
        d = AttributesFrozendict({'a': 5})
        self.assertEqual(d['a'], 5)

        with self.assertRaises(KeyError):
            d['b']

    def test_getattr(self):
        d = AttributesFrozendict({'a': 5})
        self.assertEqual(d.a, 5)

        with self.assertRaises(AttributeError):
            d.b

    def test_setitem(self):
        d = AttributesFrozendict()
        with self.assertRaises(TypeError):
            d['a'] = 5


def fct():
    pass


async def async_fct():
    pass


class TestEnsureCoroutine(unittest.TestCase):

    def test_sync_func(self):
        coro = ensure_coroutine(fct)
        assert inspect.iscoroutinefunction(coro)

    def test_async_func(self):
        coro = ensure_coroutine(async_fct)
        assert coro is async_fct

    def test_callable_class(self):
        """
        """

        class AsyncDummy:

            async def __call__(self):
                pass

        coro = ensure_coroutine(AsyncDummy)
        assert coro is AsyncDummy

    def test_functools_partial(self):
        fct_wrap = functools.partial(async_fct)
        coro = ensure_coroutine(fct_wrap)
        assert coro is fct_wrap

        fct_wrap = functools.partial(fct)
        coro = ensure_coroutine(fct_wrap)
        assert coro is not fct_wrap
