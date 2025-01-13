# -*- coding: utf-8 -*-
import pytest

from plumpy.events import set_event_loop_policy, reset_event_loop_policy


@pytest.fixture(scope='function')
def custom_event_loop_policy():
    set_event_loop_policy()
    yield
    reset_event_loop_policy()
