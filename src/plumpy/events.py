# -*- coding: utf-8 -*-
"""Event and loop related classes and functions"""

import asyncio
import sys
from typing import TYPE_CHECKING, Any, Callable, Dict, Sequence

__all__: list[str] = ['get_or_create_event_loop']

if TYPE_CHECKING:
    from .processes import Process


def get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    """Get the running event loop, or the current set loop, or create and set a new one.
    Note: aiida should never call on asyncio.get_event_loop() directly.
    """
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        pass
    try:
        # See issue https://github.com/aiidateam/plumpy/issues/336
        loop = asyncio.get_event_loop()
        if not loop.is_closed():
            return loop
    except RuntimeError:
        pass
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


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
