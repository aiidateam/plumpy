
from unittest import TestCase
from plum.process import Process
from plum.persistence.pickle_persistence import PicklePersistence
from plum.wait_ons import Checkpoint
from plum.simple_factory import SimpleFactory
import os.path


class DummyProcess(Process):
    def _run(self):
        return Checkpoint(self.finish)

    def finish(self, wait_on):
        pass


class TestPicklePersistence(TestCase):
    def setUp(self):
        import tempfile

        self.store_dir = tempfile.mkdtemp()
        self.pickle_persistence = \
            PicklePersistence(SimpleFactory(), self.store_dir)
        # Have to call on_create to make sure the Process has a PID
        self.dummy_proc = DummyProcess()
        self.dummy_proc.on_create(0)

    def tearDown(self):
        self._empty_directory()
        self.dummy_proc.on_destroy()

    def test_store_directory(self):
        self.assertEqual(self.store_dir,
                         self.pickle_persistence.store_directory)

    def test_on_starting_process(self):
        # Make sure we delete the file if it's there
        SAVE_PATH = os.path.join(self.store_dir, "0.pickle")
        if os.path.isfile(SAVE_PATH):
            os.remove(SAVE_PATH)

        self.pickle_persistence.on_process_start(self.dummy_proc)

        # Check the file exists
        self.assertTrue(os.path.isfile(SAVE_PATH))

    def test_on_waiting_process(self):
        # Make sure we delete the file if it's there
        SAVE_PATH = os.path.join(self.store_dir, "0.pickle")
        if os.path.isfile(SAVE_PATH):
            os.remove(SAVE_PATH)

        self.pickle_persistence.on_process_wait(self.dummy_proc, None)

        # Check the file exists
        self.assertTrue(os.path.isfile(SAVE_PATH))

    def test_on_finishing_process(self):
        SAVE_PATH = os.path.join(self.store_dir, "0.pickle")
        open(SAVE_PATH, 'wb')

        # Have to call this because it adds the process as a listener
        # and on_process_finish removes it
        self.pickle_persistence.persist_process(self.dummy_proc)
        self.assertTrue(os.path.isfile(SAVE_PATH))

        self.pickle_persistence.on_process_finish(self.dummy_proc, None)
        self.assertFalse(os.path.isfile(SAVE_PATH))

    def test_load_all_checkpoints(self):
        self._empty_directory()
        for i in range(0, 3):
            proc = DummyProcess()
            proc.on_create(i)
            self.pickle_persistence.on_process_start(proc)
            proc.on_destroy()

        num_cps = len(self.pickle_persistence.load_all_checkpoints())
        self.assertEqual(num_cps, 3)

    def test_save(self):
        p = DummyProcess()
        p.on_create(1234)
        self.pickle_persistence.save(p)
        save_path = os.path.join(
            self.pickle_persistence.store_directory, "1234.pickle")

        p.on_destroy()
        self.assertTrue(os.path.isfile(save_path))

    def _empty_directory(self):
        import shutil
        if os.path.isdir(self.store_dir):
            shutil.rmtree(self.store_dir)

