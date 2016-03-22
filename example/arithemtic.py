

from plum.process import Process, FunctionProcess
from plum.workflow import Workflow


def add(a, b):
    return a + b


def multiply(a, b):
    return a * b


class DynamicOutputProcess(Process):
    @staticmethod
    def _init(spec):
        spec.add_dynamic_output()

    def _run(self):
        import string
        import random
        for i in range(0, 10):
            self._out(random.choice(string.letters), i)


class PrintProcess(Process):
    @staticmethod
    def _init(spec):
        spec.add_input('value')

    def _run(self, value):
        print(value)


class TestDynamicOutput(Workflow):
    @staticmethod
    def _init(spec):
        spec.add_process(DynamicOutputProcess)
        spec.add_process(PrintProcess)

        spec.link('DynamicOutputProcess:dynamic',
                  'PrintProcess:value')


class Add(Process):
    @staticmethod
    def _init(spec):
        spec.add_input('a', default=0)
        spec.add_input('b', default=0)
        spec.add_output('value')

    def _run(self, a, b):
        self._out('value', a + b)


class Mul(Process):
    @staticmethod
    def _init(spec):
        spec.add_input('a', default=1)
        spec.add_input('b', default=1)
        spec.add_output('value')

    def _run(self, a, b):
        self._out('value', a * b)


class MulAdd(Workflow):
    @staticmethod
    def _init(spec):
        spec.add_process(Mul)
        spec.add_process(Add)
        spec.expose_inputs("Add")
        spec.add_input('c', default=0)
        spec.expose_outputs("Mul")

        spec.link(':c', 'Mul:a')
        spec.link('Add:value', 'Mul:b')


# Create a process from a function
AddFun = FunctionProcess.build(add)


class MulAddWithFun(Workflow):
    @staticmethod
    def _init(spec):
        spec.add_process(Mul)
        spec.add_process(AddFun)
        spec.expose_inputs("add")
        spec.add_input('c', default=0)
        spec.add_output('value')
        spec.expose_outputs("Mul")

        spec.link(':c', 'Mul:a')
        spec.link('add:value', 'Mul:b')


if __name__ == '__main__':
    mul_add = MulAdd.create()
    mul_add.bind('a', 2)
    mul_add.bind('b', 3)
    mul_add.bind('c', 4)
    print(mul_add.run())

    mul_add(a=2, b=3, c=4)

    mul_add = MulAdd.create()
    # Use the callable method
    print(mul_add(a=2, b=3, c=4))

    TestDynamicOutput.create().run()

