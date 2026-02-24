# -*- coding: utf-8 -*-
"""Tests for the :mod:`plumpy.events` module.
Specifically, the :func:`get_or_create_event_loop` function."""

import asyncio
from plumpy.events import get_or_create_event_loop


def test_returns_running_loop():
    """When called inside a running loop, return that loop."""
    result = None

    async def main():
        nonlocal result
        result = get_or_create_event_loop()

    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())
    assert result is loop
    loop.close()


def test_returns_existing_open_loop():
    """When no loop is running but a current loop exists and is open, return it."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        assert get_or_create_event_loop() is loop
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def test_creates_new_loop_when_closed():
    """When the current loop is closed, create and set a new one."""
    old_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(old_loop)
    old_loop.close()

    new_loop = get_or_create_event_loop()
    try:
        assert new_loop is not old_loop
        assert not new_loop.is_closed()
        assert asyncio.get_event_loop() is new_loop
    finally:
        new_loop.close()
        asyncio.set_event_loop(None)


def test_creates_new_loop_when_none_set():
    """When no current loop exists, create and set a new one."""
    asyncio.set_event_loop(None)

    loop = get_or_create_event_loop()
    try:
        assert not loop.is_closed()
        assert asyncio.get_event_loop() is loop
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def test_idempotent():
    """Consecutive calls without intervening changes return the same loop."""
    asyncio.set_event_loop(None)

    loop1 = get_or_create_event_loop()
    loop2 = get_or_create_event_loop()
    try:
        assert loop1 is loop2
    finally:
        loop1.close()
        asyncio.set_event_loop(None)
