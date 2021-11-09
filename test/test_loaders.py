# -*- coding: utf-8 -*-
"""Tests for the :mod:`plumpy.loaders` module."""
import pytest

import plumpy


class DummyClass:
    """Dummy class for testing."""
    pass


class CustomLoader(plumpy.ObjectLoader):
    """Custom implementation of ``ObjectLoader`` interface."""

    def identify_object(self, obj):
        if obj is DummyClass:
            return 'DummyClass'

    def load_object(self, identifier):
        if identifier == 'DummyClass':
            return DummyClass


def test_custom_loader():
    """Test roundtrip for a custom loader implementation."""
    loader = CustomLoader()
    cls = loader.load_object(loader.identify_object(DummyClass))
    assert cls is DummyClass


def test_default_object_roundtrip():
    """Test roundtrip for the :class:`plumpy.DefaultObjectLoader` class."""
    loader = plumpy.DefaultObjectLoader()
    identifier = loader.identify_object(DummyClass)
    cls = loader.load_object(identifier)
    assert cls is DummyClass


@pytest.mark.parametrize(
    'identifier, match', (
        ('plumpy.non_existing_module.SomeClass', r'identifier `.*` has an invalid format.'),
        ('plumpy.non_existing_module:SomeClass', r'module `.*` from identifier `.*` could not be loaded.'),
        ('plumpy.loaders:NonExistingClass', r'object `.*` form identifier `.*` could not be loaded.'),
    )
)
def test_default_object_loader_load_object_except(identifier, match):
    """Test the :meth:`plumpy.DefaultObjectLoader.load_object` when it is expected to raise."""
    loader = plumpy.DefaultObjectLoader()
    with pytest.raises(ValueError, match=match):
        loader.load_object(identifier)
