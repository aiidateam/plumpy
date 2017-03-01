from unittest import TestCase
from plum.process_manager import ProcessManager
from plum.persistence.pickle_persistence import PicklePersistence
from plum.process_monitor import MONITOR
from plum.test_utils import ProcessWithCheckpoint, WaitForSignalProcess
from plum.exceptions import LockError
import os.path


class TestPicklePersistence(TestCase):
    def setUp(self):
        import tempfile

        self.assertEqual(len(MONITOR.get_pids()), 0)

        self.store_dir = tempfile.mkdtemp()
        self.pickle_persistence = PicklePersistence(running_directory=self.store_dir)
        self.proess_manager = ProcessManager()

    def tearDown(self):
        self.assertEqual(len(MONITOR.get_pids()), 0)
        self.pickle_persistence.clear_all_persisted()
        self._empty_directory()

    def test_store_directory(self):
        self.assertEqual(self.store_dir,
                         self.pickle_persistence.store_directory)

    def test_on_create_process(self):
        proc = ProcessWithCheckpoint.new()
        self.pickle_persistence.persist_process(proc)
        save_path = self.pickle_persistence.get_running_path(proc.pid)

        self.assertTrue(os.path.isfile(save_path))

    def test_on_waiting_process(self):
        proc = WaitForSignalProcess.new()
        self.pickle_persistence.persist_process(proc)
        save_path = self.pickle_persistence.get_running_path(proc.pid)

        future = self.proess_manager.start(proc)

        # Check the file exists
        self.assertTrue(os.path.isfile(save_path))

        try:
            self.assertTrue(self.proess_manager.abort(proc.pid, timeout=1.))
        except AssertionError:
            # Already finished
            pass
        self.assertTrue(future.wait(timeout=2.))

    def test_on_finishing_process(self):
        proc = ProcessWithCheckpoint.new()
        pid = proc.pid
        self.pickle_persistence.persist_process(proc)
        running_path = self.pickle_persistence.get_running_path(proc.pid)

        self.assertTrue(os.path.isfile(running_path))
        proc.play()
        self.assertFalse(os.path.isfile(running_path))
        finished_path = \
            os.path.join(self.store_dir,
                         self.pickle_persistence.finished_directory,
                         self.pickle_persistence.pickle_filename(pid))

        self.assertTrue(os.path.isfile(finished_path))

    def test_load_all_checkpoints(self):
        self._empty_directory()
        # Create some processes
        for i in range(0, 3):
            proc = ProcessWithCheckpoint.new(pid=i)
            self.pickle_persistence.persist_process(proc)

        # Check that the number of checkpoints matches we expected
        num_cps = len(self.pickle_persistence.load_all_checkpoints())
        self.assertEqual(num_cps, 3)

    def test_save(self):
        proc = ProcessWithCheckpoint.new()
        running_path = self.pickle_persistence.get_running_path(proc.pid)
        self.pickle_persistence.save(proc)
        self.assertTrue(os.path.isfile(running_path))

    def test_persist_twice(self):
        proc = WaitForSignalProcess.new()
        self.pickle_persistence.persist_process(proc)
        future = self.proess_manager.start(proc)

        # Try persisting the process again using another persistence manager
        try:
            PicklePersistence(running_directory=self.store_dir).persist_process(proc)
        except LockError:
            pass

        proc.abort()

        assert future.wait(timeout=1.)

    def _empty_directory(self):
        import shutil
        if os.path.isdir(self.store_dir):
            shutil.rmtree(self.store_dir)
