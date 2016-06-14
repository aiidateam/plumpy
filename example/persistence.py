import time

from plum.engine.serial import SerialEngine
from plum.process import Process
from plum.util import override
from plum.wait import WaitOn


class WaitUntil(WaitOn):
    END_TIME = 'end_time'

    @classmethod
    def create_from(cls, bundle, process_factory):
        return WaitUntil(bundle[cls.END_TIME], bundle[cls.CALLBACK_NAME])

    def __init__(self, seconds, callback_name):
        super(WaitUntil, self).__init__(callback_name)
        self._end_time = time.time() + seconds

    @override
    def is_ready(self):
        return time.time() >= self._end_time

    @override
    def save_instance_state(self, out_state):
        super(WaitUntil, self).save_instance_state(out_state)
        out_state[self.END_TIME] = self._end_time


class Add(Process):
    @classmethod
    def _define(cls, spec):
        spec.input('a', default=0)
        spec.input('b', default=0)
        spec.output('value')

    def __init__(self, pid):
        super(Add, self).__init__(pid)
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
    add = Add(0)

    exec_engine = SerialEngine(persistence=FilePersistenceManager())
    exec_engine.run_and_block(add, {'a': 2, 'b': 3})


