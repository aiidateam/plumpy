from unittest import TestCase
from plum.persistence.bundle import Bundle
from plum.wait_ons import *


class TestWaitOns(TestCase):
    def test_save_instance_state(self):
        cp = Checkpoint(None)
        waits = (cp, WaitOnAll(None, (cp,)), WaitOnAny(None, (cp,)))

        for wait in waits:
            b = Bundle()
            wait.save_instance_state(b)
