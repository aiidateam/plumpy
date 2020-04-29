# -*- coding: utf-8 -*-
import plumpy
from test import test_utils
import unittest
import yaml

from . import utils


class SaveEmpty(plumpy.Savable):
    pass


@plumpy.auto_persist('test', 'test_method')
class Save1(plumpy.Savable):

    def __init__(self):
        self.test = 'sup yp'
        self.test_method = self.m

    def m():
        pass


@plumpy.auto_persist('test')
class Save(plumpy.Savable):

    def __init__(self):
        self.test = Save1()


class TestSavable(unittest.TestCase):

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
        self.assertDictEqual(saved_state1, saved_state2)

    def _save_round_trip_with_loader(self, savable):
        """
        Do a round trip:
        1) Save `savables` state
        2) Recreate from the saved state
        3) Save the state of the recreated `Savable`
        4) Compare the two saved states (they should match)

        :type savable: :class:`plumpy.Savable`
        """
        object_loader = plumpy.get_object_loader()
        saved_state1 = savable.save(plumpy.LoadSaveContext(object_loader))
        loaded = savable.recreate_from(saved_state1)
        saved_state2 = loaded.save(plumpy.LoadSaveContext(object_loader))
        saved_state3 = loaded.save()
        self.assertDictEqual(saved_state1, saved_state2)
        self.assertNotEqual(saved_state1, saved_state3)


class TestBundle(utils.TestCaseWithLoop):

    def test_bundle_load_context(self):
        """ Check that the loop from the load context is used """
        proc = test_utils.DummyProcess(loop=self.loop)
        bundle = plumpy.Bundle(proc)

        loop2 = plumpy.new_event_loop()
        proc2 = bundle.unbundle(plumpy.LoadSaveContext(loop=loop2))
        self.assertIs(loop2, proc2.loop())

    def test_bundle_yaml(self):
        bundle = plumpy.Bundle(Save1())
        represent = yaml.dump({'bundle': bundle})

        bundle_loaded = yaml.load(represent, Loader=yaml.Loader)['bundle']
        self.assertIsInstance(bundle_loaded, plumpy.Bundle)
        self.assertDictEqual(bundle_loaded, Save1().save())
