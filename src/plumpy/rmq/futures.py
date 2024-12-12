# -*- coding: utf-8 -*-
"""
Module containing future related methods and classes
"""

import asyncio
import concurrent.futures
from asyncio.futures import _chain_future, _copy_future_state  # type: ignore[attr-defined]
from typing import Any

import kiwipy

__all__ = ['chain', 'copy_future', 'wrap_to_kiwi_future']

copy_future = _copy_future_state
chain = _chain_future


def wrap_to_kiwi_future(future: asyncio.Future[Any]) -> kiwipy.Future:
    """Wrap to concurrent.futures.Future object."""
    if isinstance(future, concurrent.futures.Future):
        return future
    assert asyncio.isfuture(future), f'concurrent.futures.Future is expected, got {future!r}'

    new_future = kiwipy.Future()
    _chain_future(future, new_future)
    return new_future
