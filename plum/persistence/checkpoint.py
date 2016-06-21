
from plum.persistence.bundle import Bundle


class Checkpoint(object):
    def __init__(self,):
        self.pid = None
        self.process_class = None
        self.process_instance_state = Bundle()
        self.wait_on_instance_state = Bundle()

    def __str__(self):
        desc = []
        desc.append("pid: {}".format(self.pid))
        desc.append("process class: {}".format(self.process_class))
        desc.append("process instance state:\n{}".format(
            self.process_instance_state))
        desc.append("wait-on instance state:\n{}".format(
            self.wait_on_instance_state))
        return "\n".join(desc)
