# -*- coding: utf-8 -*-
"""Greenlet-based async/sync bridge utilities for plumpy.

This module provides utilities to bridge synchronous and asynchronous code
using greenlets. It enables synchronous process code to await async operations
while the event loop is running.
"""

import asyncio
import contextvars
import sys
import threading
from contextvars import ContextVar
from typing import Any, Awaitable, Callable, TypeVar

from greenlet import getcurrent, greenlet

__all__ = ['in_worker_greenlet', 'run_in_greenlet', 'run_in_thread', 'run_until_complete', 'sync_await']

_T = TypeVar('_T')

# Track if we're inside a worker greenlet
_IN_WORKER_GREENLET: ContextVar[bool] = ContextVar('_in_worker_greenlet', default=False)


def in_worker_greenlet() -> bool:
    """Check if currently executing inside a worker greenlet.

    Returns:
        True if inside a worker greenlet (can use sync_await to switch to parent),
        False otherwise.
    """
    return _IN_WORKER_GREENLET.get()


def run_in_thread(awaitable_factory: Callable[[], Awaitable[_T]]) -> _T:
    """Run an awaitable in a separate thread with its own event loop.

    This is used for nested process execution when the main event loop
    is already running. The awaitable is created fresh in the thread
    to avoid cross-loop issues.

    Args:
        awaitable_factory: A callable that creates the awaitable to run.

    Returns:
        The result of the awaitable.
    """
    result_holder: list = []
    exception_holder: list = []

    def thread_target() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            awaitable = awaitable_factory()
            result_holder.append(loop.run_until_complete(awaitable))
        except BaseException as e:
            exception_holder.append(e)
        finally:
            loop.close()

    thread = threading.Thread(target=thread_target)
    thread.start()
    thread.join()

    if exception_holder:
        raise exception_holder[0]

    return result_holder[0] if result_holder else None  # type: ignore


def sync_await(awaitable: Awaitable[_T]) -> _T:
    """Await an async operation from synchronous code inside a worker greenlet.

    This function allows synchronous code to "await" an async operation by
    switching to the parent greenlet which performs the actual await.

    Must only be called from within a worker greenlet created by run_in_greenlet().

    Args:
        awaitable: The awaitable (coroutine, task, future) to await.

    Returns:
        The result of the awaited operation.

    Raises:
        RuntimeError: If called outside of a worker greenlet context.
    """
    if not _IN_WORKER_GREENLET.get():
        raise RuntimeError(
            'sync_await() must be called from within a worker greenlet. '
            'Use in_worker_greenlet() to check before calling.'
        )

    return getcurrent().parent.switch(awaitable)


async def run_in_greenlet(fn: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
    """Run a sync function in a greenlet, allowing it to await via sync_await().

    This async function creates a greenlet to run a synchronous function.
    The sync function can call sync_await(some_awaitable) to perform async
    operations while appearing to be synchronous code.

    Args:
        fn: The synchronous function to run.
        *args: Positional arguments to pass to fn.
        **kwargs: Keyword arguments to pass to fn.

    Returns:
        The return value of fn.
    """
    result_holder: list = []
    exception_holder: list = []

    # Capture the current context so we can propagate it into the worker
    # greenlet. Greenlets start with a fresh empty context, so without this,
    # ContextVars set by the caller (e.g. PROCESS_STACK) would not be visible.
    ctx = contextvars.copy_context()

    def worker() -> None:
        """Worker greenlet that runs the sync function."""

        def _inner() -> None:
            _IN_WORKER_GREENLET.set(True)
            try:
                result_holder.append(fn(*args, **kwargs))
            except BaseException as e:
                exception_holder.append(e)

        ctx.run(_inner)

    worker_greenlet = greenlet(worker)
    switch_result = worker_greenlet.switch()

    # Process awaitable requests until worker completes
    while not worker_greenlet.dead:
        if isinstance(switch_result, Awaitable):
            try:
                value = await switch_result
            except BaseException:
                switch_result = worker_greenlet.throw(*sys.exc_info())
            else:
                switch_result = worker_greenlet.switch(value)
        else:
            break

    if exception_holder:
        raise exception_holder[0]

    return result_holder[0] if result_holder else None  # type: ignore


def run_until_complete(loop: asyncio.AbstractEventLoop, awaitable: Awaitable[_T]) -> _T:
    """Run an awaitable to completion, handling nested event loop scenarios.

    This function provides backwards compatibility for code that previously
    relied on nest_asyncio to allow nested run_until_complete() calls.

    If the loop is not running, uses standard run_until_complete().
    If the loop is running and we're in a worker greenlet, uses sync_await().
    If the loop is running but not in a greenlet, runs in a separate thread.

    Args:
        loop: The event loop to use.
        awaitable: The awaitable to run.

    Returns:
        The result of the awaitable.
    """
    if loop.is_running():
        if in_worker_greenlet():
            return sync_await(awaitable)
        else:
            # Last resort: run in a separate thread with its own event loop.
            # This assumes the awaitable is thread-safe. Process.execute() does
            # NOT use this path — it raises instead — because process execution
            # is generally not thread-safe (e.g. thread-local DB sessions).
            return run_in_thread(lambda: awaitable)
    else:
        return loop.run_until_complete(awaitable)
