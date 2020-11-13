# -*- coding: utf-8 -*-
"""Event and loop related classes and functions"""
import sys
import asyncio

__all__ = [
    'new_event_loop', 'set_event_loop', 'get_event_loop', 'run_until_complete', 'set_event_loop_policy',
    'reset_event_loop_policy', 'PlumpyEventLoopPolicy'
]

get_event_loop = asyncio.get_event_loop  # pylint: disable=invalid-name


def set_event_loop(*args, **kwargs):
    raise NotImplementedError('this method is not implemented because `plumpy` uses a single reentrant loop')


def new_event_loop(*args, **kwargs):
    raise NotImplementedError('this method is not implemented because `plumpy` uses a single reentrant loop')


class PlumpyEventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    """Custom event policy that always returns the same event loop that is made reentrant by ``nest_asyncio``."""

    _loop = None

    def get_event_loop(self):
        """Return the patched event loop."""
        import nest_asyncio

        if self._loop is None:
            self._loop = super().get_event_loop()
            nest_asyncio.apply(self._loop)

        return self._loop


def set_event_loop_policy():
    """Enable plumpy's event loop policy that will make event loop's reentrant."""
    asyncio.set_event_loop_policy(PlumpyEventLoopPolicy())


def reset_event_loop_policy():
    """Reset the event loop policy to the default."""
    loop = get_event_loop()

    # pylint: disable=protected-access
    cls = loop.__class__
    cls._run_once = cls._run_once_orig
    cls.run_forever = cls._run_forever_orig
    cls.run_until_complete = cls._run_until_complete_orig

    del cls._check_running
    del cls._check_runnung  # typo in Python 3.7 source
    del cls._run_once_orig
    del cls._run_forever_orig
    del cls._run_until_complete_orig
    del cls._nest_patched
    # pylint: enable=protected-access

    asyncio.set_event_loop_policy(None)


def run_until_complete(future, loop=None):
    loop = loop or get_event_loop()
    return loop.run_until_complete(future)


class ProcessCallback:
    """Object returned by callback registration methods."""

    __slots__ = ('_callback', '_args', '_kwargs', '_process', '_cancelled', '__weakref__')

    def __init__(self, process, callback, args, kwargs):
        self._process = process
        self._callback = callback
        self._args = args
        self._kwargs = kwargs
        self._cancelled = False

    def cancel(self):
        if not self._cancelled:
            self._cancelled = True
            self._done()

    def cancelled(self):
        return self._cancelled

    async def run(self):
        """Run the callback"""
        if not self._cancelled:
            try:
                await self._callback(*self._args, **self._kwargs)
            except Exception:  # pylint: disable=broad-except
                exc_info = sys.exc_info()
                self._process.callback_excepted(self._callback, exc_info[1], exc_info[2])
            finally:
                self._done()

    def _done(self):
        self._cleanup()

    def _cleanup(self):
        self._process = None
        self._callback = None
        self._args = None
        self._kwargs = None
