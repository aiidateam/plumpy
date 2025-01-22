from plumpy.persistence import Savable, load
from tests.utils import DummyProcess, ProcessListenerTester


def test_process_listener_savable():
    proc = DummyProcess()
    pl = ProcessListenerTester(proc, ('killed'))
    assert isinstance(pl, Savable)

    saved = pl.save()
    loaded = load(saved_state=saved)
    saved2 = loaded.save()

    assert saved == saved2
