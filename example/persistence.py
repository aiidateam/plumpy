import time
import threading
from plum.process import Process
from plum.util import override
from plum.wait import WaitOn


class WaitUntil(WaitOn):
    END_TIME = 'end_time'

    @override
    def init(self, wait_for):
        self._end_time = time.time() + wait_for
        self._timer = threading.Timer(wait_for, self._timeout)
        self._timer.start()

    @override
    def load_instance_state(self, bundle):
        self._end_time = bundle[self.END_TIME]
        self._timer = threading.Timer(
            time.time() - self._end_time, self._timeout)
        self._timer.start()

    @override
    def save_instance_state(self, out_state):
        super(WaitUntil, self).save_instance_state(out_state)
        out_state[self.END_TIME] = self._end_time

    def _timeout(self):
        self._timer.join()
        self._timer = None
        self.done(True)


class Add(Process):
    @classmethod
    def define(cls, spec):
        spec.input('a', default=0)
        spec.input('b', default=0)
        spec.output('value')

    def __init__(self):
        super(Add, self).__init__()
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



