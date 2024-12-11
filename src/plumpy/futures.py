# -*- coding: utf-8 -*-
"""
Module containing future related methods and classes
"""

import asyncio
import contextlib
from typing import Any, Awaitable, Callable, Generator, Optional

__all__ = ['CancellableAction', 'create_task', 'create_task', 'capture_exceptions']


class InvalidFutureError(Exception):
    """Exception for when a future or action is in an invalid state"""


Future = asyncio.Future


@contextlib.contextmanager
def capture_exceptions(future: Future[Any], ignore: tuple[type[BaseException], ...] = ()) -> Generator[None, Any, None]:
    """
    Capture any exceptions in the context and set them as the result of the given future

    :param future: The future to the exception on
    :param ignore: An optional list of exception types to ignore, these will be raised and not set on the future
    """
    try:
        yield
    except ignore:
        raise
    except Exception as exception:
        future.set_exception(exception)


class CancellableAction(Future):
    """
    An action that can be launched and potentially cancelled
    """

    def __init__(self, action: Callable[..., Any], cookie: Any = None):
        super().__init__()
        self._action = action
        self._cookie = cookie

    @property
    def cookie(self) -> Any:
        """A cookie that can be used to correlate the actions with something"""
        return self._cookie

    def run(self, *args: Any, **kwargs: Any) -> None:
        """Run the action

        :param args: the positional arguments to the action
        :param kwargs: the keyword arguments to the action
        """
        if self.done():
            raise InvalidFutureError('Action has already been ran')

        try:
            with capture_exceptions(self):
                self.set_result(self._action(*args, **kwargs))
        finally:
            self._action = None  # type: ignore


def create_task(coro: Callable[[], Awaitable[Any]], loop: Optional[asyncio.AbstractEventLoop] = None) -> Future:
    """
    Schedule a call to a coro in the event loop and wrap the outcome
    in a future.

    :param coro: a function which creates the coroutine to schedule
    :param loop: the event loop to schedule it in
    :return: the future representing the outcome of the coroutine

    """
    loop = loop or asyncio.get_event_loop()

    future = loop.create_future()

    async def run_task() -> None:
        with capture_exceptions(future):
            res = await coro()
            future.set_result(res)

    asyncio.run_coroutine_threadsafe(run_task(), loop)
    return future
