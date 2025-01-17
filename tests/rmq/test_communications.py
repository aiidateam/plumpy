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
def subscriber():
    """Return an instance of mocked `Subscriber`."""

    class Subscriber:
        """Test class that mocks a subscriber."""

        def __call__(self):
            pass

    return Subscriber()


def test_add_rpc_subscriber(_coordinator, subscriber):
    """Test the `LoopCommunicator.add_rpc_subscriber` method."""
    assert _coordinator.add_rpc_subscriber(subscriber) is not None

    identifier = 'identifier'
    assert _coordinator.add_rpc_subscriber(subscriber, identifier) == identifier


def test_remove_rpc_subscriber(_coordinator, subscriber):
    """Test the `LoopCommunicator.remove_rpc_subscriber` method."""
    identifier = _coordinator.add_rpc_subscriber(subscriber)
    _coordinator.remove_rpc_subscriber(identifier)


def test_add_broadcast_subscriber(_coordinator, subscriber):
    """Test the `LoopCommunicator.add_broadcast_subscriber` method."""
    assert _coordinator.add_broadcast_subscriber(subscriber) is not None

    identifier = 'identifier'
    assert _coordinator.add_broadcast_subscriber(subscriber, identifier=identifier) == identifier


def test_remove_broadcast_subscriber(_coordinator, subscriber):
    """Test the `LoopCommunicator.remove_broadcast_subscriber` method."""
    identifier = _coordinator.add_broadcast_subscriber(subscriber)
    _coordinator.remove_broadcast_subscriber(identifier)


def test_add_task_subscriber(_coordinator, subscriber):
    """Test the `LoopCommunicator.add_task_subscriber` method."""
    assert _coordinator.add_task_subscriber(subscriber) is not None


def test_remove_task_subscriber(_coordinator, subscriber):
    """Test the `LoopCommunicator.remove_task_subscriber` method."""
    identifier = _coordinator.add_task_subscriber(subscriber)
    _coordinator.remove_task_subscriber(identifier)
