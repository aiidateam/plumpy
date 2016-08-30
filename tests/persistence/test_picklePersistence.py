
from unittest import TestCase
from plum.process import ProcessState
from plum.persistence.pickle_persistence import PicklePersistence
from plum.persistence.bundle import Bundle
from plum.process_monitor import MONITOR
from plum.test_utils import ProcessWithCheckpoint, TEST_PROCESSES
import os.path


class TestPicklePersistence(TestCase):
    def setUp(self):
        import tempfile

        self.assertEqual(len(MONITOR.get_pids()), 0)

        self.store_dir = tempfile.mkdtemp()
        self.pickle_persistence = PicklePersistence(running_directory=self.store_dir)

    def tearDown(self):
        self.assertEqual(len(MONITOR.get_pids()), 0)
        self._empty_directory()

    def test_store_directory(self):
        self.assertEqual(self.store_dir,
                         self.pickle_persistence.store_directory)

    def test_on_starting_process(self):
        proc = ProcessWithCheckpoint.new_instance()
        self.pickle_persistence.persist_process(proc)
        save_path = self.pickle_persistence.get_running_path(proc.pid)

        self.assertTrue(os.path.isfile(save_path))
        proc.stop(True)
        self.assertTrue(os.path.isfile(save_path))

    def test_on_waiting_process(self):
        proc = ProcessWithCheckpoint.new_instance()
        self.pickle_persistence.persist_process(proc)
        save_path = self.pickle_persistence.get_running_path(proc.pid)

        proc.run_until(ProcessState.WAITING)

        # Check the file exists
        self.assertTrue(os.path.isfile(save_path))

        proc.stop(True)

    def test_on_finishing_process(self):
        proc = ProcessWithCheckpoint.new_instance()
        pid = proc.pid
        self.pickle_persistence.persist_process(proc)
        running_path = self.pickle_persistence.get_running_path(proc.pid)

        self.assertTrue(os.path.isfile(running_path))
        proc.run_until_complete()
        self.assertFalse(os.path.isfile(running_path))
        finished_path =\
            os.path.join(self.store_dir,
                         self.pickle_persistence.finished_directory,
                         self.pickle_persistence.pickle_filename(pid))

        self.assertTrue(os.path.isfile(finished_path))

    def test_load_all_checkpoints(self):
        self._empty_directory()
        # Create some processes
        for i in range(0, 3):
            proc = ProcessWithCheckpoint.new_instance(pid=i)
            self.pickle_persistence.persist_process(proc)
            proc.stop(True)

        # Check that the number of checkpoints matches we we expected
        num_cps = len(self.pickle_persistence.load_all_checkpoints())
        self.assertEqual(num_cps, 3)

    def test_save(self):
        proc = ProcessWithCheckpoint.new_instance()
        running_path = self.pickle_persistence.get_running_path(proc.pid)
        self.pickle_persistence.save(proc)
        self.assertTrue(os.path.isfile(running_path))

        proc.stop(True)

    def test_save_and_load(self):
        for ProcClass in TEST_PROCESSES:
            proc = ProcClass.new_instance()
            while proc.state is not ProcessState.DESTROYED:
                # Create a bundle manually
                b = Bundle()
                proc.save_instance_state(b)

                self.pickle_persistence.save(proc)
                b2 = self.pickle_persistence.load_checkpoint(proc.pid)

                self.assertEqual(b, b2, "Bundle not the same after loading from pickle")

                # The process may crash, so catch it here
                try:
                    proc.tick()
                except BaseException:
                    break

    def _empty_directory(self):
        import shutil
        if os.path.isdir(self.store_dir):
            shutil.rmtree(self.store_dir)

