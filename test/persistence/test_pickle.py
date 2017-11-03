import tempfile

if getattr(tempfile, 'TemporaryDirectory', None) is None:
    from backports import tempfile

from plum.persistence.pickle_persistence import PicklePersister
from plum.test_utils import ProcessWithCheckpoint
from test.util import TestCaseWithLoop


class TestPicklePersister(TestCaseWithLoop):
    def test_on_create_process(self):
        process = self.loop.create(ProcessWithCheckpoint)

        with tempfile.TemporaryDirectory() as directory:
            persister = PicklePersister(directory)
            persister.save_checkpoint(process)

            bundle = persister.load_checkpoint(process.pid)

            recreated = bundle.unbundle(self.loop)
