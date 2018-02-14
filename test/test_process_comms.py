import unittest
import plumpy
from plumpy import test_utils
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


class TestProcessLauncher(unittest.TestCase):
    def test_continue(self):
        loop = plumpy.new_event_loop()
        persister = plumpy.InMemoryPersister()
        load_context = plumpy.LoadContext(loop=loop)
        launcher = plumpy.ProcessLauncher(persister=persister, load_context=load_context)

        process = test_utils.DummyProcess()
        persister.save_checkpoint(process)

        future = launcher._continue(plumpy.create_continue_body(process.pid))
        result = loop.run_sync(lambda: future)
