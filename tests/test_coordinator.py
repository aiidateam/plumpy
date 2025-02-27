# -*- coding: utf-8 -*-
from plumpy.coordinator import Coordinator
from .utils import MockCoordinator


def test_mock_coordinator():
    assert isinstance(MockCoordinator, Coordinator)
