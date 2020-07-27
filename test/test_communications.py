# -*- coding: utf-8 -*-
"""Tests for the :mod:`plumpy.communications` module."""
import pytest

from kiwipy import CommunicatorHelper
from plumpy.communications import LoopCommunicator


class Subscriber:
    """Test class that mocks a subscriber."""


class Communicator(CommunicatorHelper):

    def task_send(self, task, no_reply=False):
        pass

    def rpc_send(self, recipient_id, msg):
        pass

    def broadcast_send(self, body, sender=None, subject=None, correlation_id=None):
        pass


@pytest.fixture
def loop_communicator():
    """Return an instance of `LoopCommunicator`."""
    return LoopCommunicator(Communicator())


@pytest.fixture
def subscriber():
    """Return an instance of `Subscriber`."""
    return Subscriber()


def test_add_rpc_subscriber(loop_communicator, subscriber):
    """Test the `LoopCommunicator.add_rpc_subscriber` method."""
    assert loop_communicator.add_rpc_subscriber(subscriber) is not None

    identifier = 'identifier'
    assert loop_communicator.add_rpc_subscriber(subscriber, identifier) == identifier


def test_remove_rpc_subscriber(loop_communicator, subscriber):
    """Test the `LoopCommunicator.remove_rpc_subscriber` method."""
    identifier = loop_communicator.add_rpc_subscriber(subscriber)
    loop_communicator.remove_rpc_subscriber(identifier)


def test_add_broadcast_subscriber(loop_communicator, subscriber):
    """Test the `LoopCommunicator.add_broadcast_subscriber` method."""
    assert loop_communicator.add_broadcast_subscriber(subscriber) is not None

    identifier = 'identifier'
    assert loop_communicator.add_broadcast_subscriber(subscriber, identifier) == identifier


def test_remove_broadcast_subscriber(loop_communicator, subscriber):
    """Test the `LoopCommunicator.remove_broadcast_subscriber` method."""
    identifier = loop_communicator.add_broadcast_subscriber(subscriber)
    loop_communicator.remove_broadcast_subscriber(identifier)


def test_add_task_subscriber(loop_communicator, subscriber):
    """Test the `LoopCommunicator.add_task_subscriber` method."""
    assert loop_communicator.add_task_subscriber(subscriber) is not None


def test_remove_task_subscriber(loop_communicator, subscriber):
    """Test the `LoopCommunicator.remove_task_subscriber` method."""
    identifier = loop_communicator.add_task_subscriber(subscriber)
    loop_communicator.remove_task_subscriber(identifier)
