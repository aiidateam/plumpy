# -*- coding: utf-8 -*-
"""Tests for the :mod:`plumpy.rmq.communications` module."""

import pytest

import kiwipy
from plumpy.rmq.communications import LoopCommunicator
from . import RmqCoordinator


@pytest.fixture
def _coordinator():
    """Return an instance of `LoopCommunicator`."""

    class _Communicator(kiwipy.CommunicatorHelper):
        def task_send(self, task, no_reply=False):
            pass

        def rpc_send(self, recipient_id, msg):
            pass

        def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
            pass

    comm = LoopCommunicator(_Communicator())
    coordinator = RmqCoordinator(comm)

    yield coordinator

    coordinator.close()


@pytest.fixture
def receiver_fn():
    """Return an instance of mocked `Subscriber`."""

    class Subscriber:
        """Test class that mocks a subscriber."""

        def __call__(self):
            pass

    return Subscriber()


def test_hook_rpc_receiver(_coordinator, receiver_fn):
    """Test the `LoopCommunicator.add_rpc_receiver` method."""
    assert _coordinator.hook_rpc_receiver(receiver_fn) is not None

    identifier = 'identifier'
    assert _coordinator.hook_rpc_receiver(receiver_fn, identifier) == identifier


def test_unhook_rpc_receiver(_coordinator, receiver_fn):
    """Test the `LoopCommunicator.remove_rpc_subscriber` method."""
    identifier = _coordinator.hook_rpc_receiver(receiver_fn)
    _coordinator.unhook_rpc_receiver(identifier)


def test_hook_broadcast_receiver(_coordinator, receiver_fn):
    """Test the coordinator hook_broadcast_receiver which calls
    `LoopCommunicator.add_broadcast_subscriber` method."""
    assert _coordinator.hook_broadcast_receiver(receiver_fn) is not None

    identifier = 'identifier'
    assert _coordinator.hook_broadcast_receiver(receiver_fn, identifier=identifier) == identifier


def test_unhook_broadcast_receiver(_coordinator, receiver_fn):
    """Test the `LoopCommunicator.remove_broadcast_subscriber` method."""
    identifier = _coordinator.hook_broadcast_receiver(receiver_fn)
    _coordinator.unhook_broadcast_receiver(identifier)


def test_hook_task_receiver(_coordinator, receiver_fn):
    """Test the hook_task_receiver calls `LoopCommunicator.add_task_subscriber` method."""
    assert _coordinator.hook_task_receiver(receiver_fn) is not None


def test_unhook_task_receiver(_coordinator, receiver_fn):
    """Test the `LoopCommunicator.remove_task_subscriber` method."""
    identifier = _coordinator.hook_task_receiver(receiver_fn)
    _coordinator.unhook_task_receiver(identifier)
