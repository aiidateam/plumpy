import plumpy

class HelloWorld(plumpy.Process):

    @classmethod
    def define(cls, spec):
        super(HelloWorld, cls).define(spec)
        spec.input('name', default='World', required=True)
        spec.output("greeting", valid_type=str)

    def run(self, **kwargs):
        s = "Hello {:}!".format(self.inputs.name)
        self.out("greeting", s)
        return plumpy.Stop(None, True)

if __name__ == "__main__":
    p = HelloWorld(inputs={'name': 'foobar'})
    print("Process State: {:}".format(p.state))

    p.execute()

    print("Process State: {:}".format(p.state))
    print("{:}".format(p.outputs['greeting']))

    # default inputs
    p = HelloWorld()
    p.execute()
    print("{:}".format(p.outputs['greeting']))
