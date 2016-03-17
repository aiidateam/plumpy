

from plum.process import Process, FunctionProcess
from plum.workflow import Workflow


def add(a, b):
    return a + b


def multiply(a, b):
    return a * b


class Add(Process):
    @staticmethod
    def _init(spec):
        spec.add_input('a', default=0)
        spec.add_input('b', default=0)
        spec.add_output('value')

    def _run(self, a, b):
        return {'value': a + b}


class Mul(Process):
    @staticmethod
    def _init(spec):
        spec.add_input('a', default=1)
        spec.add_input('b', default=1)
        spec.add_output('value')

    def _run(self, a, b):
        return {'value': a * b}


#AddFun = FunctionProcess.build(add)

class MulAdd(Workflow):
    @staticmethod
    def _init(spec):
        spec.add_input('e', default=0)
        spec.add_input('f', default=0)
        spec.add_input('g', default=0)
        spec.add_output('value')

        spec.add_process(Mul)
        spec.add_process(Add)
        spec.link(':e', 'Add:a')
        spec.link(':f', 'Add:b')
        spec.link(':g', 'Mul:a')
        spec.link('Add:value', 'Mul:b')
        spec.link('Mul:value', ':value')


# class MulAddWithFun(Workflow):
#     @staticmethod
#     def _init(spec):
#         spec.add_input('e', default='0')
#         spec.add_input('f', default='0')
#         spec.add_input('g', default='0')
#         spec.add_output('value')
#
#         spec.add_process(Mul)
#         spec.add_process(AddFun)
#         spec.link(':e', 'add:a')
#         spec.link(':f', 'add:b')
#         spec.link(':g', 'Mul:a')
#         spec.link('add:value', 'Mul:b')
#         spec.link('Mul:value', ':value')


if __name__ == '__main__':
    mul_add = MulAdd.create()
    mul_add.bind('e', 2)
    mul_add.bind('f', 3)
    mul_add.bind('g', 4)
    print(mul_add.run())

