# -*- coding: utf-8 -*-
import plumpy


class AddAndMulWF(plumpy.WorkChain):

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input('ini', valid_type=float, default=1.0)
        spec.input('add', valid_type=int, required=True)
        spec.input('mul', valid_type=int, required=True)
        spec.output('result', valid_type=float)
        spec.outline(
            cls.add,
            cls.mul,
        )

    def add(self):
        self.ctx.addresult = self.inputs.ini + self.inputs.add

    def mul(self):
        result = self.ctx.addresult * self.inputs.mul
        self.out('result', result)


def launch():
    workchain = AddAndMulWF(inputs={'ini': 10.0, 'add': 1, 'mul': 2})
    workchain.execute()
    print(workchain.outputs['result'])  # prints 22.0


if __name__ == '__main__':
    launch()
