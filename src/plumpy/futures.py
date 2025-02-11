# -*- coding: utf-8 -*-
"""
Module containing future related methods and classes
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any, Callable, Generator


class InvalidFutureError(Exception):
    """Exception for when a future or action is in an invalid state"""


Future = asyncio.Future


@contextlib.contextmanager
def capture_exceptions(future, ignore: tuple[type[BaseException], ...] = ()) -> Generator[None, Any, None]:  # type: ignore[no-untyped-def]
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
