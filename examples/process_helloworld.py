# -*- coding: utf-8 -*-
import plumpy


class HelloWorld(plumpy.Process):

    @classmethod
    def define(cls, spec):
        super(HelloWorld, cls).define(spec)
        spec.input('name', default='World', required=True)
        spec.output('greeting', valid_type=str)

    def run(self):
        self.out('greeting', 'Hello {:}!'.format(self.inputs.name))
        return plumpy.Stop(None, True)


def launch():
    process = HelloWorld(inputs={'name': 'foobar'})
    print('Process State: {:}'.format(process.state))
    process.execute()

    print('Process State: {:}'.format(process.state))
    print('{:}'.format(process.outputs['greeting']))

    # default inputs
    process = HelloWorld()
    process.execute()
    print('{:}'.format(process.outputs['greeting']))


if __name__ == '__main__':
    launch()
