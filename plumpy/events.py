# -*- coding: utf-8 -*-
"""Event and loop related classes and functions"""
import sys
from tornado import ioloop
import tornado.gen

__all__ = ['new_event_loop', 'set_event_loop', 'get_event_loop', 'run_until_complete']

get_event_loop = ioloop.IOLoop.current  # pylint: disable=invalid-name


def new_event_loop():
    loop = ioloop.IOLoop()
    loop.make_current()
    return loop


def set_event_loop(loop):
    if loop is None:
        ioloop.IOLoop.clear_instance()
    else:
        loop.make_current()


def run_until_complete(future, loop=None):
    if loop is None:
        loop = get_event_loop()

    return loop.run_sync(lambda: future)


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
