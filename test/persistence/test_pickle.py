import tempfile

if getattr(tempfile, 'TemporaryDirectory', None) is None:
    from backports import tempfile

from plum.persisters import PersistedCheckpoint, PicklePersister
from plum.test_utils import ProcessWithCheckpoint
from test.utils import TestCaseWithLoop


class TestPicklePersister(TestCaseWithLoop):

    def test_save_load_roundtrip(self):
        """
        Test the PicklePersister by taking a dummpy process, saving a checkpoint
        and recreating it from the same checkpoint
        """
        process = ProcessWithCheckpoint()

        with tempfile.TemporaryDirectory() as directory:
            persister = PicklePersister(directory)
            persister.save_checkpoint(process)

            bundle = persister.load_checkpoint(process.pid)
            recreated = bundle.unbundle(loop=self.loop)

    def test_get_checkpoints_without_tags(self):
        """
        """
        process_a = ProcessWithCheckpoint()
        process_b = ProcessWithCheckpoint()

        checkpoint_a = PersistedCheckpoint(process_a.pid, None)
        checkpoint_b = PersistedCheckpoint(process_b.pid, None)

        checkpoints = [checkpoint_a, checkpoint_b]

        with tempfile.TemporaryDirectory() as directory:
            persister = PicklePersister(directory)
            persister.save_checkpoint(process_a)
            persister.save_checkpoint(process_b)

            retrieved_checkpoints = persister.get_checkpoints()

            self.assertEquals(set(retrieved_checkpoints), set(checkpoints))

    def test_get_checkpoints_with_tags(self):
        """
        """
        process_a = ProcessWithCheckpoint()
        process_b = ProcessWithCheckpoint()
        tag_a = 'tag_a'
        tag_b = 'tag_b'

        checkpoint_a = PersistedCheckpoint(process_a.pid, tag_a)
        checkpoint_b = PersistedCheckpoint(process_b.pid, tag_b)

        checkpoints = [checkpoint_a, checkpoint_b]

        with tempfile.TemporaryDirectory() as directory:
            persister = PicklePersister(directory)
            persister.save_checkpoint(process_a, tag=tag_a)
            persister.save_checkpoint(process_b, tag=tag_b)

            retrieved_checkpoints = persister.get_checkpoints()

            self.assertEquals(set(retrieved_checkpoints), set(checkpoints))

    def test_get_process_checkpoints(self):
        """
        """
        process_a = ProcessWithCheckpoint()
        process_b = ProcessWithCheckpoint()

        checkpoint_a1 = PersistedCheckpoint(process_a.pid, '1')
        checkpoint_a2 = PersistedCheckpoint(process_a.pid, '2')
        checkpoint_b1 = PersistedCheckpoint(process_b.pid, '1')
        checkpoint_b2 = PersistedCheckpoint(process_b.pid, '2')

        checkpoints = [checkpoint_a1, checkpoint_a2]

        with tempfile.TemporaryDirectory() as directory:
            persister = PicklePersister(directory)
            persister.save_checkpoint(process_a, tag='1')
            persister.save_checkpoint(process_a, tag='2')
            persister.save_checkpoint(process_b, tag='1')
            persister.save_checkpoint(process_b, tag='2')

            retrieved_checkpoints = persister.get_process_checkpoints(process_a.pid)

            self.assertEquals(set(retrieved_checkpoints), set(checkpoints))

    def test_delete_process_checkpoints(self):
        """
        """
        process_a = ProcessWithCheckpoint()
        process_b = ProcessWithCheckpoint()

        checkpoint_a1 = PersistedCheckpoint(process_a.pid, '1')
        checkpoint_a2 = PersistedCheckpoint(process_a.pid, '2')
        checkpoint_b1 = PersistedCheckpoint(process_b.pid, '1')
        checkpoint_b2 = PersistedCheckpoint(process_b.pid, '2')

        with tempfile.TemporaryDirectory() as directory:
            persister = PicklePersister(directory)
            persister.save_checkpoint(process_a, tag='1')
            persister.save_checkpoint(process_a, tag='2')
            persister.save_checkpoint(process_b, tag='1')
            persister.save_checkpoint(process_b, tag='2')

            checkpoints = [checkpoint_a1, checkpoint_a2]
            retrieved_checkpoints = persister.get_process_checkpoints(process_a.pid)

            self.assertEquals(set(retrieved_checkpoints), set(checkpoints))

            persister.delete_process_checkpoints(process_a.pid)

            checkpoints = []
            retrieved_checkpoints = persister.get_process_checkpoints(process_a.pid)

            self.assertEquals(set(retrieved_checkpoints), set(checkpoints))

    def test_delete_checkpoint(self):
        """
        """
        process_a = ProcessWithCheckpoint()
        process_b = ProcessWithCheckpoint()

        checkpoint_a1 = PersistedCheckpoint(process_a.pid, '1')
        checkpoint_a2 = PersistedCheckpoint(process_a.pid, '2')
        checkpoint_b1 = PersistedCheckpoint(process_b.pid, '1')
        checkpoint_b2 = PersistedCheckpoint(process_b.pid, '2')

        checkpoints = [checkpoint_a1, checkpoint_a2, checkpoint_b1]

        with tempfile.TemporaryDirectory() as directory:
            persister = PicklePersister(directory)
            persister.save_checkpoint(process_a, tag='1')
            persister.save_checkpoint(process_a, tag='2')
            persister.save_checkpoint(process_b, tag='1')
            persister.save_checkpoint(process_b, tag='2')

            checkpoints = [checkpoint_a1, checkpoint_a2, checkpoint_b1, checkpoint_b2]
            retrieved_checkpoints = persister.get_checkpoints()

            self.assertEquals(set(retrieved_checkpoints), set(checkpoints))

            persister.delete_checkpoint(process_a.pid, tag='2')

            checkpoints = [checkpoint_a1, checkpoint_b1, checkpoint_b2]
            retrieved_checkpoints = persister.get_checkpoints()

            self.assertEquals(set(retrieved_checkpoints), set(checkpoints))

            persister.delete_checkpoint(process_b.pid, tag='1')

            checkpoints = [checkpoint_a1, checkpoint_b2]
            retrieved_checkpoints = persister.get_checkpoints()

            self.assertEquals(set(retrieved_checkpoints), set(checkpoints))