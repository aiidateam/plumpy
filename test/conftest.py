# -*- coding: utf-8 -*-
import pytest


@pytest.fixture(scope='session')
def set_event_loop_policy():
    from plumpy import set_event_loop_policy
    set_event_loop_policy()
