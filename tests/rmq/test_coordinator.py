# -*- coding: utf-8 -*-
from plumpy.coordinator import Coordinator
from . import RmqCoordinator


def test_mock_coordinator():
    assert isinstance(RmqCoordinator, Coordinator)
