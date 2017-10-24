import apricotpy
import os.path
from plum.process import ProcessState
from plum.persistence.pickle_persistence import PicklePersistence
from plum.test_utils import ProcessWithCheckpoint, WaitForSignalProcess
from plum.persistence import pickle_persistence
from plum import loop_factory
from plum.wait_ons import run_until
from test.util import TestCase


class TestPicklePersistence(TestCase):
    def setUp(self):
        import tempfile

        super(TestPicklePersistence, self).setUp()

        self.loop = loop_factory()
        apricotpy.set_event_loop(self.loop)

        self.store_dir = tempfile.mkdtemp()
        self.pickle_persistence = \
            self.loop.create(PicklePersistence, running_directory=self.store_dir)

    def tearDown(self):
        super(TestPicklePersistence, self).tearDown()
        self.pickle_persistence.reset_persisted(delete=True)
        self._empty_directory()

    def test_store_directory(self):
        self.assertEqual(self.store_dir, self.pickle_persistence.store_directory)

    def test_on_create_process(self):
        proc = self.loop.create(ProcessWithCheckpoint)
        self.pickle_persistence.persist_process(proc)
        save_path = self.pickle_persistence.get_save_file(proc)

        self.assertTrue(os.path.isfile(save_path))

    def test_on_waiting_process(self):
        proc = self.loop.create(WaitForSignalProcess)
        self.pickle_persistence.persist_process(proc)
        save_path = self.pickle_persistence.get_save_file(proc)

        # Wait - Run the process and wait until it is waiting
        run_until(proc, ProcessState.WAITING, self.loop)

        # Check the file exists
        self.assertTrue(os.path.isfile(save_path))

    def test_on_finishing_process(self):
        proc = self.loop.create(ProcessWithCheckpoint)

        # Persist and check save file exists
        self.pickle_persistence.persist_process(proc)
        save_file = self.pickle_persistence.get_save_file(proc)
        self.assertTrue(os.path.isfile(save_file))

        # Run until end and make sure it is deleted
        self.loop.run_until_complete(proc)
        self.assertFalse(os.path.isfile(save_file))

        # Check that it's been moved into the finish folder
        finished_path = os.path.join(
            self.store_dir,
            self.pickle_persistence.finished_directory,
            self.pickle_persistence.pickle_filename(proc)
        )
        self.assertTrue(os.path.isfile(finished_path))

    def test_load_all_checkpoints(self):
        # Create some processes and pickles
        for _ in range(3):
            proc = self.loop.create(ProcessWithCheckpoint)
            self.pickle_persistence.save(proc)

        # Check that the number of checkpoints matches we expected
        num_cps = len(self.pickle_persistence.load_all_checkpoints())
        self.assertEqual(num_cps, 3)

    def test_save(self):
        proc = self.loop.create(ProcessWithCheckpoint)
        running_path = self.pickle_persistence.get_save_file(proc)
        self.pickle_persistence.save(proc)
        self.assertTrue(os.path.isfile(running_path))

    def test_persist_twice(self):
        proc = self.loop.create(WaitForSignalProcess)
        self.pickle_persistence.persist_process(proc)

        # Try persisting the process again using another persistence manager
        with self.assertRaises(pickle_persistence.LockException):
            pp = self.loop.create(PicklePersistence, running_directory=self.store_dir)
            pp.save(proc)

    def _empty_directory(self):
        import shutil
        if os.path.isdir(self.store_dir):
            shutil.rmtree(self.store_dir)
