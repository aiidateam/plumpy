

from plum.process import Process
from plum.workflow import Workflow
from plum.serial_engine import SerialEngine
from plum.persistence.file_persistence import FilePersistenceManager
from plum.wait import WaitOn
import time


class WaitFor(WaitOn):
    END_TIME = 'end_time'

    @classmethod
    def create_from(cls, bundle, exec_engine):
        return WaitFor(bundle[cls.END_TIME], bundle[cls.CALLBACK_NAME])

    def __init__(self, seconds, callback_name):
        super(WaitFor, self).__init__(callback_name)
        self._end_time = time.time() + seconds

    def is_ready(self):
        return time.time() >= self._end_time

    def save_instance_state(self, bundle, exec_engine):
        super(WaitFor, self).save_instance_state(bundle, exec_engine)
        bundle[self.END_TIME] = self._end_time


class Add(Process):
    @staticmethod
    def _define(spec):
        spec.input('a', default=0)
        spec.input('b', default=0)
        spec.output('value')

    def __init__(self):
        super(Add, self).__init__()
        self._a = None
        self._b = None

    def _run(self, a, b):
        self._a = a
        self._b = b
        return WaitFor(10, self._finish.__name__)

    def _finish(self, wait_on):
        self._out('value', self._a + self._b)


class Mul(Process):
    @staticmethod
    def _define(spec):
        spec.input('a', default=1)
        spec.input('b', default=1)
        spec.output('value')

    def _run(self, a, b):
        self._out('value', a * b)


class MulAdd(Workflow):
    @staticmethod
    def _define(spec):
        spec.process(Mul)
        spec.process(Add)
        spec.exposed_inputs("Add")
        spec.input('c', default=0)
        spec.exposed_outputs("Mul")

        spec.link(':c', 'Mul:a')
        spec.link('Add:value', 'Mul:b')



if __name__ == '__main__':
    mul_add = MulAdd.create()

    exec_engine = SerialEngine(persistence=FilePersistenceManager())
    exec_engine.run(mul_add, {'a': 2, 'b': 3, 'c': 4})


