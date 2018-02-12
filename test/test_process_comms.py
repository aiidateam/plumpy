import unittest
from plumpy import test_utils
from plumpy import process_comms


class TestProcessReceiver(unittest.TestCase):
    def test_pause_play(self):
        """ Check that the receiver correctly pauses and plays """
        proc = test_utils.DummyProcess()
        receiver = process_comms.ProcessReceiver(proc)
        self.assertFalse(proc.paused)

        # Pause it
        result = receiver(process_comms.PAUSE_MSG)
        self.assertTrue(result)
        self.assertTrue(proc.paused)

        # Play it
        result = receiver(process_comms.PLAY_MSG)
        self.assertTrue(result)
        self.assertFalse(proc.paused)
