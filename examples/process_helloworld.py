# -*- coding: utf-8 -*-
import plumpy


class HelloWorld(plumpy.Process):

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input('name', default='World', required=True)
        spec.output('greeting', valid_type=str)

    def run(self):
        self.out('greeting', f'Hello {self.inputs.name}!')
        return plumpy.Stop(None, True)


def launch():
    process = HelloWorld(inputs={'name': 'foobar'})
    print(f'Process State: {process.state}')
    process.execute()

    print(f'Process State: {process.state}')
    print(f"{process.outputs['greeting']}")

    # default inputs
    process = HelloWorld()
    process.execute()
    print(f"{process.outputs['greeting']}")


if __name__ == '__main__':
    launch()
