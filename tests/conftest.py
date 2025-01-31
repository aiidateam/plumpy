# -*- coding: utf-8 -*-
import pytest

from plumpy.events import set_event_loop_policy, reset_event_loop_policy


@pytest.fixture(scope='function')
def custom_event_loop_policy():
    """This is the fixture for changing the event loop of synchronous tests.
    If using `@pytest.mark.asyncio`, the event loop can be set by `event_loop_policy`
    fixture of pytest-asyncio.
    """
    set_event_loop_policy()
    yield
    reset_event_loop_policy()
