import plumpy
import unittest


class TestSavable(unittest.TestCase):
    def test_empty_savable(self):
        class Save(plumpy.Savable):
            pass

        self._save_round_trip(Save())

    def test_auto_persist(self):
        @plumpy.auto_persist('test')
        class Save(plumpy.Savable):
            def __init__(self):
                self.test = 'sup yp'

        self._save_round_trip(Save())

    @unittest.skip("Need to fix nested include_class_name=False")
    def test_auto_persist_savable(self):
        @plumpy.auto_persist('test')
        class Save1(plumpy.Savable):
            def __init__(self):
                self.test = 'sup yp'

        @plumpy.auto_persist('test')
        class Save(plumpy.Savable):
            def __init__(self):
                self.test = Save1()

        self._save_round_trip(Save())

    def _save_round_trip(self, savable):
        """
        Do a round trip:
        1) Save `savables` state
        2) Recreate from the saved state
        3) Save the state of the recreated `Savable`
        4) Compare the two saved states (they should match)

        :type savable: :class:`plumpy.Savable`
        """
        saved_state1 = savable.save(include_class_name=False)
        loaded = savable.recreate_from(saved_state1)
        saved_state2 = loaded.save(include_class_name=False)
        self.assertDictEqual(saved_state1, saved_state2)
