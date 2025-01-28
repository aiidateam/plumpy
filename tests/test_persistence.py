# -*- coding: utf-8 -*-
import asyncio
from typing import Any

import yaml

import plumpy
from plumpy.loaders import DefaultObjectLoader, ObjectLoader
from plumpy.persistence import auto_load, auto_persist, auto_save, ensure_object_loader
from plumpy.utils import SAVED_STATE_TYPE

from . import utils

# FIXME: test auto_load can precisely load auto_persist with nested items


@auto_persist()
class SaveEmpty:
    @classmethod
    def recreate_from(cls, saved_state, load_context=None):
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = auto_load(cls, saved_state, load_context)
        return obj

    def save(self, loader=None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = auto_save(self)

        return out_state


@plumpy.auto_persist('test', 'test_method')
class Save1:
    def __init__(self):
        self.test = 'sup yp'
        self.test_method = self.m

    def m():
        pass

    @classmethod
    def recreate_from(cls, saved_state, load_context=None):
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = auto_load(cls, saved_state, load_context)
        return obj

    def save(self, loader=None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = auto_save(self, loader)

        return out_state


@plumpy.auto_persist('test')
class Save:
    def __init__(self):
        self.test = Save1()

    @classmethod
    def recreate_from(cls, saved_state, load_context=None):
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = auto_load(cls, saved_state, load_context)
        return obj

    def save(self, loader=None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = auto_save(self, loader)

        return out_state


class CustomObjectLoader(DefaultObjectLoader):
    def load_object(self, identifier: str) -> Any:
        return super().load_object(identifier)

    def identify_object(self, obj: Any) -> str:
        return super().identify_object(obj)


class TestSavable:
    def test_empty_savable(self):
        self._save_round_trip(SaveEmpty())

    def test_auto_persist(self):
        self._save_round_trip(Save1())
        self._save_round_trip_with_loader(Save1())

    def test_auto_persist_savable(self):
        self._save_round_trip(Save())
        self._save_round_trip_with_loader(Save())

    def _save_round_trip(self, savable):
        """
        Do a round trip:
        1) Save `savables` state
        2) Recreate from the saved state
        3) Save the state of the recreated `Savable`
        4) Compare the two saved states (they should match)

        :type savable: :class:`plumpy.Savable`
        """
        saved_state1 = savable.save()
        loaded = savable.recreate_from(saved_state1)
        saved_state2 = loaded.save()
        assert saved_state1 == saved_state2

    def _save_round_trip_with_loader(self, savable):
        """
        Do a round trip, use a custom loader:
        1) Save `savables` state
        2) Recreate from the saved state
        3) Save the state of the recreated `Savable`
        4) Compare the two saved states (they should match)

        :type savable: :class:`plumpy.Savable`
        """
        object_loader = CustomObjectLoader()
        saved_state1 = savable.save(object_loader)
        loaded = savable.recreate_from(saved_state1)
        saved_state2 = loaded.save(object_loader)
        saved_state3 = loaded.save()
        assert saved_state1 == saved_state2
        assert saved_state1 != saved_state3


class TestBundle:
    def test_bundle_load_context(self):
        """Check that the loop from the load context is used"""
        loop1 = asyncio.get_event_loop()
        proc = utils.DummyProcess(loop=loop1)
        bundle = plumpy.Bundle(proc)

        loop2 = asyncio.new_event_loop()
        proc2 = bundle.unbundle(plumpy.LoadSaveContext(loop=loop2))
        assert loop2 is proc2.loop

    def test_bundle_yaml(self):
        bundle = plumpy.Bundle(Save1())
        represent = yaml.dump({'bundle': bundle})

        bundle_loaded = yaml.load(represent, Loader=yaml.Loader)['bundle']
        assert isinstance(bundle_loaded, plumpy.Bundle)
        assert bundle_loaded == Save1().save()
