# -*- coding: utf-8 -*-
from plumpy.event_helper import EventHelper
from plumpy.persistence import Savable, load
from tests.utils import DummyProcess, ProcessListenerTester


def test_event_helper_savable():
    eh = EventHelper(ProcessListenerTester)

    proc = DummyProcess()
    pl1 = ProcessListenerTester(proc, ('killed'))
    pl2 = ProcessListenerTester(proc, ('paused'))
    eh.add_listener(pl1)
    eh.add_listener(pl2)

    assert isinstance(eh, Savable)

    saved = eh.save()
    loaded = load(saved_state=saved)
    saved2 = loaded.save()

    assert saved == saved2
