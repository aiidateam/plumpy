import plumpy

class AddAndMulWF(plumpy.WorkChain):

    @classmethod
    def define(cls, spec):
        super(AddAndMulWF, cls).define(spec)
        spec.input("ini", valid_type=float, default=1.0)
        spec.input("add", valid_type=int, required=True)
        spec.input("mul", valid_type=int,required=True)
        spec.output("result", valid_type=float)
        spec.outline(
            cls.add,
            cls.mul,
        )

    def add(self):
        self.ctx.addresult = self.inputs.ini + self.inputs.add

    def mul(self):
        r = self.ctx.addresult * self.inputs.mul
        self.out("result", r)


if __name__ == "__main__":
    wf = AddAndMulWF(inputs={"ini": 10.0, "add": 1, "mul": 2})
    wf.execute()

    print(wf.outputs["result"])
    # output:
    # 22.0
