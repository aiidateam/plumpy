from abc import ABCMeta


class Defaults(object):
    TASK_QUEUE = 'plum.task_queue'
    TASK_CONTROL_EXCHANGE = 'plum.task_control'
    STATUS_EXCHANGE = 'plum.status_updates'
    STATUS_REQUEST_EXCHANGE = 'plum.status.status_request'


class Subscriber(object):
    """
    An abstract class that defines an interface that subscribers should conform
    to.
    """
    __metaclass__ = ABCMeta

    def start(self):
        pass

    def poll(self, timeout=None):
        pass

    def stop(self):
        pass
