import time
import threading
from plum.process import Process
from plum.util import override
from plum.wait import WaitOn, WaitEvent, Interrupted


class WaitUntil(WaitOn):
    END_TIME = 'end_time'

    @override
    def __init__(self, wait_for):
        super(WaitUntil, self).__init__()

        self._end_time = time.time() + wait_for
        self._timeout = WaitEvent()

    @override
    def wait(self, timeout=None):
        wait_time = self._end_time - time.time()

        if timeout is not None and timeout < wait_time:
            wait_time = timeout
            will_timeout = True
        else:
            will_timeout = False

        self._timeout.wait(wait_time)
        return not will_timeout

    @override
    def interrupt(self):
        self._timeout.interrupt()

    @override
    def load_instance_state(self, saved_state):
        super(WaitUntil, self).load_instance_state(saved_state)

        self._end_time = saved_state[self.END_TIME]
        self._timeout = WaitEvent()

    @override
    def save_instance_state(self, out_state):
        super(WaitUntil, self).save_instance_state(out_state)
        out_state[self.END_TIME] = self._end_time


class Add(Process):
    @classmethod
    def define(cls, spec):
        spec.input('a', default=0)
        spec.input('b', default=0)
        spec.output('value')

    def __init__(self, inputs, pid, logger=None):
        super(Add, self).__init__(inputs, pid, logger)
        self._a = None
        self._b = None

    @override
    def _run(self, a, b):
        self._a = a
        self._b = b
        return WaitUntil(2, self._finish.__name__)

    def _finish(self, wait_on):
        self.out('value', self._a + self._b)


if __name__ == '__main__':
    Add.run()
