
from plum.persistence.bundle import Bundle


class Checkpoint(object):
    def __init__(self, process, wait_on=None):
        assert process.pid is not None,\
            "Process must have a PID to create checkpoint"
        self._pid = process.pid
        self._process_class = process.__class__
        self._process_instance_state = Bundle()
        self._wait_on_instance_state = Bundle()

        process.save_instance_state(self._process_instance_state)
        if wait_on is not None:
            wait_on.save_instance_state(self._wait_on_instance_state)

    @property
    def pid(self):
        return self._pid

    @property
    def process_class(self):
        return self._process_class

    @property
    def process_instance_state(self):
        return self._process_instance_state

    @property
    def wait_on_instance_state(self):
        return self._wait_on_instance_state
