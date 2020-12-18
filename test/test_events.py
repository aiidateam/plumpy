# -*- coding: utf-8 -*-
"""Tests for the :mod:`plumpy.events` module."""
import asyncio
import pathlib

import pytest

from plumpy import set_event_loop_policy, reset_event_loop_policy, PlumpyEventLoopPolicy, set_event_loop, new_event_loop


def test_set_event_loop_policy():
    """Test the ``plumpy.set_event_loop_policy``."""
    assert not isinstance(asyncio.get_event_loop_policy(), PlumpyEventLoopPolicy)
    set_event_loop_policy()
    assert isinstance(asyncio.get_event_loop_policy(), PlumpyEventLoopPolicy)


def test_reset_event_loop_policy():
    """Test the ``plumpy.reset_event_loop_policy``."""
    set_event_loop_policy()
    assert isinstance(asyncio.get_event_loop_policy(), PlumpyEventLoopPolicy)
    reset_event_loop_policy()
    assert not isinstance(asyncio.get_event_loop_policy(), PlumpyEventLoopPolicy)


def test_get_event_loop():
    """Test that ``asyncio.get_event_loop`` returns same loop instance every time it is called once policy is set."""
    set_event_loop_policy()
    assert isinstance(asyncio.get_event_loop_policy(), PlumpyEventLoopPolicy)
    assert asyncio.get_event_loop() is asyncio.get_event_loop()
    assert asyncio.get_event_loop()._nest_patched is not None


def test_set_event_loop():
    """Test the ``set_event_loop`` raises ``NotImplementedError``."""
    with pytest.raises(NotImplementedError):
        set_event_loop()


def test_new_event_loop():
    """Test the ``new_event_loop`` raises ``NotImplementedError``."""
    with pytest.raises(NotImplementedError):
        new_event_loop()


def test_get_event_loop_jupyter_notebook(nb_regression):
    """Test that ``asyncio.get_event_loop`` returns same loop instance every time it is called once policy is set."""
    nb_regression.diff_color_words = False
    nb_regression.diff_ignore = ('/metadata/language_info/version',)

    with open(pathlib.Path(__file__).parent / 'notebooks' / 'get_event_loop.ipynb') as handle:
        nb_regression.check(handle)
