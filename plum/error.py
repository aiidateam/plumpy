

class FastForwardError(Exception):
    """
    Exception raised when there is a problem that prevents a process from
    fast-forwarding.
    """
    def __init__(self, msg):
        super(FastForwardError, self).__init__(msg)


