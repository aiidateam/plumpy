# -*- coding: utf-8 -*-
# mypy: disable-error-code="no-untyped-def, no-untyped-call"
"""Module containing future related methods and classes"""

from __future__ import annotations

import asyncio
import concurrent.futures
from typing import Any

import kiwipy

__all__ = ['unwrap_kiwi_future', 'wrap_to_concurrent_future']


def _convert_future_exc(exc):
    exc_class = type(exc)
    if exc_class is concurrent.futures.CancelledError:
        return asyncio.exceptions.CancelledError(*exc.args)
    elif exc_class is concurrent.futures.TimeoutError:
        return asyncio.exceptions.TimeoutError(*exc.args)
    elif exc_class is concurrent.futures.InvalidStateError:
        return asyncio.exceptions.InvalidStateError(*exc.args)
    else:
        return exc


def _set_concurrent_future_state(concurrent, source):
    """Copy state from a future to a concurrent.futures.Future."""
    assert source.done()
    if source.cancelled():
        concurrent.cancel()
    if not concurrent.set_running_or_notify_cancel():
        return
    exception = source.exception()
    if exception is not None:
        concurrent.set_exception(_convert_future_exc(exception))
    else:
        result = source.result()
        concurrent.set_result(result)


def _copy_future_state(source, dest):
    """Internal helper to copy state from another Future.

    The other Future may be a concurrent.futures.Future.
    """
    assert source.done()
    if dest.cancelled():
        return
    assert not dest.done()
    if source.cancelled():
        dest.cancel()
    else:
        exception = source.exception()
        if exception is not None:
            dest.set_exception(_convert_future_exc(exception))
        else:
            result = source.result()
            dest.set_result(result)


def _chain_future(source, destination):
    """Chain two futures so that when one completes, so does the other.

    The result (or exception) of source will be copied to destination.
    If destination is cancelled, source gets cancelled too.
    Compatible with both asyncio.Future and concurrent.futures.Future.
    """
    if not asyncio.isfuture(source) and not isinstance(source, concurrent.futures.Future):
        raise TypeError('A future is required for source argument')
    if not asyncio.isfuture(destination) and not isinstance(destination, concurrent.futures.Future):
        raise TypeError('A future is required for destination argument')
    source_loop = asyncio.Future.get_loop(source) if asyncio.isfuture(source) else None
    dest_loop = asyncio.Future.get_loop(destination) if asyncio.isfuture(destination) else None

    def _set_state(future, other):
        if asyncio.isfuture(future):
            _copy_future_state(other, future)
        else:
            _set_concurrent_future_state(future, other)

    def _call_check_cancel(destination):
        if destination.cancelled():
            if source_loop is None or source_loop is dest_loop:
                source.cancel()
            else:
                source_loop.call_soon_threadsafe(source.cancel)

    def _call_set_state(source):
        if destination.cancelled() and dest_loop is not None and dest_loop.is_closed():
            return
        if dest_loop is None or dest_loop is source_loop:
            _set_state(destination, source)
        else:
            if dest_loop.is_closed():
                return
            dest_loop.call_soon_threadsafe(_set_state, destination, source)

    destination.add_done_callback(_call_check_cancel)
    source.add_done_callback(_call_set_state)


def wrap_to_concurrent_future(future: asyncio.Future[Any]) -> kiwipy.Future:
    """Wrap to concurrent.futures.Future object. (the function is adapted from asyncio.future.wrap_future).
    The function `_chain_future`, `_copy_future_state` is from asyncio future module."""
    if isinstance(future, concurrent.futures.Future):
        return future
    assert asyncio.isfuture(future), f'concurrent.futures.Future is expected, got {future!r}'

    new_future = kiwipy.Future()
    _chain_future(future, new_future)
    return new_future


# XXX: this required in aiida-core, see if really need this unwrap.
def unwrap_kiwi_future(future: kiwipy.Future) -> kiwipy.Future:
    """
    Create a kiwi future that represents the final results of a nested series of futures,
    meaning that if the futures provided itself resolves to a future the returned
    future will not resolve to a value until the final chain of futures is not a future
    but a concrete value.  If at any point in the chain a future resolves to an exception
    then the returned future will also resolve to that exception.

    :param future: the future to unwrap
    :return: the unwrapping future

    """
    unwrapping = kiwipy.Future()

    def unwrap(fut: kiwipy.Future) -> None:
        if fut.cancelled():
            unwrapping.cancel()
        else:
            with kiwipy.capture_exceptions(unwrapping):
                result = fut.result()
                if isinstance(result, kiwipy.Future):
                    result.add_done_callback(unwrap)
                else:
                    unwrapping.set_result(result)

    future.add_done_callback(unwrap)
    return unwrapping
