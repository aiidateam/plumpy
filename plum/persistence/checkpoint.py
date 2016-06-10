
from plum.persistence.bundle import Bundle


class Checkpoint(object):
    def __init__(self,):
        self.pid = None
        self.process_class = None
        self.process_instance_state = Bundle()
        self.wait_on_instance_state = Bundle()
