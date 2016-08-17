
from unittest import TestCase
from plum.process import Process
from plum.persistence.pickle_persistence import PicklePersistence
from plum.wait_ons import Checkpoint
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
        self.pickle_persistence = PicklePersistence(directory=self.store_dir)
        # Have to call on_create to make sure the Process has a PID
        self.dummy_proc = DummyProcess()
        self.dummy_proc.perform_create()
        self.save_path = \
            self.pickle_persistence.get_running_path(self.dummy_proc.pid)
        # Make sure we delete the file if it's there
        if os.path.isfile(self.save_path):
            os.remove(self.save_path)

    def tearDown(self):
        self._empty_directory()

    def test_store_directory(self):
        self.assertEqual(self.store_dir,
                         self.pickle_persistence.store_directory)

    def test_on_starting_process(self):
        self.pickle_persistence.on_process_run(self.dummy_proc)

        # Check the file exists
        self.assertTrue(os.path.isfile(self.save_path))

    def test_on_waiting_process(self):
        self.pickle_persistence.on_process_wait(self.dummy_proc, None)

        # Check the file exists
        self.assertTrue(os.path.isfile(self.save_path))

    def test_on_finishing_process(self):
        # Have to call this because it adds the process as a listener
        # and on_process_finish removes it
        self.pickle_persistence.persist_process(self.dummy_proc)
        self.assertTrue(os.path.isfile(self.save_path))

        self.pickle_persistence.on_process_destroy(self.dummy_proc)
        self.assertFalse(os.path.isfile(self.save_path))

    def test_load_all_checkpoints(self):
        self._empty_directory()
        for i in range(0, 3):
            proc = DummyProcess()
            proc.on_create(i, None, None)
            self.pickle_persistence.on_process_run(proc)
            proc.on_destroy()

        num_cps = len(self.pickle_persistence.load_all_checkpoints())
        self.assertEqual(num_cps, 3)

    def test_save(self):
        self.pickle_persistence.save(self.dummy_proc)
        self.assertTrue(os.path.isfile(self.save_path))

    def _empty_directory(self):
        import shutil
        if os.path.isdir(self.store_dir):
            shutil.rmtree(self.store_dir)

