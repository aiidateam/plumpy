import tempfile
from plum import loop_factory
from plum.persistence.pickle import PicklePersister
from plum.test_utils import ProcessWithCheckpoint
from test.util import TestCase


class TestPicklePersister(TestCase):
    def setUp(self):
        super(TestPicklePersister, self).setUp()
        self.loop = loop_factory()

    def tearDown(self):
        super(TestPicklePersister, self).tearDown()

    def test_on_create_process(self):
        process = self.loop.create(ProcessWithCheckpoint)

        with tempfile.mkdtemp() as directory:
            persister = PicklePersister(directory)
            persister.save_checkpoint(process)

            bundle = persister.load_checkpoint(process.pid)

            recreated = bundle.unbundle(self.loop)