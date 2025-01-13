# -*- coding: utf-8 -*-
"""Event and loop related classes and functions"""

import asyncio
import sys
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Sequence

__all__ = [
    'PlumpyEventLoopPolicy',
    'get_event_loop',
    'new_event_loop',
    'reset_event_loop_policy',
    'run_until_complete',
    'set_event_loop',
    'set_event_loop_policy',
]

if TYPE_CHECKING:
    from .processes import Process

get_event_loop = asyncio.get_event_loop


def create_running_loop() -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    return loop


def set_event_loop(*args: Any, **kwargs: Any) -> None:
    raise NotImplementedError('this method is not implemented because `plumpy` uses a single reentrant loop')


def new_event_loop(*args: Any, **kwargs: Any) -> asyncio.AbstractEventLoop:
    raise NotImplementedError('this method is not implemented because `plumpy` uses a single reentrant loop')


class PlumpyEventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    """Custom event policy that always returns the same event loop that is made reentrant by ``nest_asyncio``."""

    _loop: Optional[asyncio.AbstractEventLoop] = None

    def new_event_loop(self) -> asyncio.AbstractEventLoop:
        import nest_asyncio

        self._loop = super().new_event_loop()
        nest_asyncio.apply(self._loop)

        return self._loop

    def get_event_loop(self) -> asyncio.AbstractEventLoop:
        """Return the patched event loop."""
        return self._loop or self.new_event_loop()


def set_event_loop_policy() -> None:
    """Enable plumpy's event loop policy that will make event loop's reentrant."""
    asyncio.set_event_loop_policy(PlumpyEventLoopPolicy())
    # Need to call the following explicitly for `asyncio.get_event_loop` to start calling the method of the new policy
    # in case an loop is already active.
    asyncio.get_event_loop_policy().get_event_loop()


def reset_event_loop_policy() -> None:
    """Reset the event loop policy to the default."""

    loop = asyncio.get_event_loop()

    cls = loop.__class__

    del cls._check_running  # type: ignore
    del cls._nest_patched  # type: ignore

    asyncio.set_event_loop_policy(None)


def run_until_complete(future: asyncio.Future, loop: Optional[asyncio.AbstractEventLoop] = None) -> Any:
    loop = loop or asyncio.get_event_loop()
    return loop.run_until_complete(future)


class ProcessCallback:
    """Object returned by callback registration methods."""

    __slots__ = ('__weakref__', '_args', '_callback', '_cancelled', '_kwargs', '_process')

    def __init__(
        self, process: 'Process', callback: Callable[..., Any], args: Sequence[Any], kwargs: Dict[str, Any]
    ) -> None:
        self._process: 'Process' = process
        self._callback: Callable[..., Any] = callback
        self._args: Sequence[Any] = args
        self._kwargs: Dict[str, Any] = kwargs
        self._cancelled: bool = False

    def cancel(self) -> None:
        if not self._cancelled:
            self._cancelled = True
            self._done()

    def cancelled(self) -> bool:
        return self._cancelled

    async def run(self) -> None:
        """Run the callback"""
        if not self._cancelled:
            try:
                await self._callback(*self._args, **self._kwargs)
            except Exception:
                exc_info = sys.exc_info()
                self._process.callback_excepted(self._callback, exc_info[1], exc_info[2])
            finally:
                self._done()

    def _done(self) -> None:
        self._cleanup()

    def _cleanup(self) -> None:
        self._process = None  # type: ignore
        self._callback = None  # type: ignore
        self._args = None  # type: ignore
        self._kwargs = None  # type: ignore
