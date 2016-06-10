
from unittest import TestCase
from plum.process import Process
from plum.persistence.pickle_persistence import PicklePersistence,\
    _STORE_DIRECTORY
from plum.wait_ons import Checkpoint
from plum.simple_factory import SimpleFactory
import os.path


class DummyProcess(Process):
    def _run(self):
        return Checkpoint(self.finish.__name__)

    def finish(self, wait_on):
        pass


class TestPicklePersistence(TestCase):
    def setUp(self):
        self.pickle_persistence = PicklePersistence(SimpleFactory())
        # Have to call on_create to make sure the Process has a PID
        self.dummy_proc = DummyProcess()
        self.dummy_proc.on_create(0)

    def test_on_starting_process(self):
        # Make sure we delete the file if it's there
        SAVE_PATH = os.path.join(_STORE_DIRECTORY, "0.pickle")
        if os.path.isfile(SAVE_PATH):
            os.remove(SAVE_PATH)

        self.pickle_persistence.on_process_start(self.dummy_proc)

        # Check the file exists
        self.assertTrue(os.path.isfile(SAVE_PATH))

    def test_on_waiting_process(self):
        # Make sure we delete the file if it's there
        SAVE_PATH = os.path.join(_STORE_DIRECTORY, "0.pickle")
        if os.path.isfile(SAVE_PATH):
            os.remove(SAVE_PATH)

        self.pickle_persistence.on_process_wait(self.dummy_proc, None)

        # Check the file exists
        self.assertTrue(os.path.isfile(SAVE_PATH))

    def test_on_finishing_process(self):
        SAVE_PATH = os.path.join(_STORE_DIRECTORY, "0.pickle")
        open(SAVE_PATH, 'wb')

        # Have to call this because it adds the process as a listener
        # and on_process_finish removes it
        self.pickle_persistence.persist_process(self.dummy_proc)
        self.assertTrue(os.path.isfile(SAVE_PATH))

        self.pickle_persistence.on_process_finish(self.dummy_proc, None)
        self.assertFalse(os.path.isfile(SAVE_PATH))

    def test_load_all_checkpoints(self):
        for i in range(0, 3):
            proc = DummyProcess()
            proc.on_create(i)
            self.pickle_persistence.on_process_start(proc)
        self.assertEqual(len(self.pickle_persistence.load_all_checkpoints()), 3)
