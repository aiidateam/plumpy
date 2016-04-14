

from plum.process import Process, FunctionProcess
from plum.workflow import Workflow
from plum.parallel import MultithreadedExecutionEngine


def add(a, b):
    return a + b


def multiply(a, b):
    return a * b


class DynamicOutputProcess(Process):
    @staticmethod
    def _define(spec):
        spec.dynamic_output()

    def _run(self):
        import string
        import random
        for i in range(0, 10):
            self._out(random.choice(string.letters), i)


class PrintProcess(Process):
    @staticmethod
    def _define(spec):
        spec.input('value')

    def _run(self, value):
        print(value)


class TestDynamicOutput(Workflow):
    @staticmethod
    def _define(spec):
        spec.process(DynamicOutputProcess)
        spec.process(PrintProcess)

        spec.link('DynamicOutputProcess:dynamic',
                  'PrintProcess:value')


class Add(Process):
    @staticmethod
    def _define(spec):
        spec.input('a', default=0)
        spec.input('b', default=0)
        spec.output('value')

    def _run(self, a, b):
        self._out('value', a + b)


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


# Create a process from a function
AddFun = FunctionProcess.build(add)


class MulAddWithFun(Workflow):
    @staticmethod
    def _define(spec):
        spec.process(Mul)
        spec.process(AddFun)
        spec.exposed_inputs("add")
        spec.input('c', default=0)
        spec.output('value')
        spec.exposed_outputs("Mul")

        spec.link(':c', 'Mul:a')
        spec.link('add:value', 'Mul:b')


if __name__ == '__main__':
    mul_add = MulAdd.create()
    print(mul_add.run({'a': 2, 'b': 3, 'c': 4}))

    mul_add(a=2, b=3, c=4)

    exec_engine = MultithreadedExecutionEngine()
    exec_engine.run(mul_add, {'a': 2, 'b': 3, 'c': 4})

    TestDynamicOutput.create().run()

