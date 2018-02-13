from plumpy.ports import PortNamespace, InputPort
from plumpy import ProcessSpec
from .utils import TestCase


class StrSubtype(str):
    pass


class TestProcessSpec(TestCase):

    def setUp(self):
        self.spec = ProcessSpec()

    def test_get_port_namespace_base(self):
        """
        Get the root, inputs and outputs port namespaces of the ProcessSpec
        """
        ports = self.spec.ports
        input_ports = self.spec.inputs
        output_ports = self.spec.outputs

        self.assertTrue(input_ports.name, self.spec.NAME_INPUTS_PORT_NAMESPACE)
        self.assertTrue(output_ports.name, self.spec.NAME_OUTPUTS_PORT_NAMESPACE)

    def test_dynamic_output(self):
        self.spec.outputs.dynamic = True
        self.spec.outputs.valid_type = str
        self.assertTrue(self.spec.validate_outputs({'dummy': 'foo'})[0])
        self.assertTrue(self.spec.validate_outputs({'dummy': StrSubtype('bar')})[0])
        self.assertFalse(self.spec.validate_outputs({'dummy': 5})[0])

        # Remove dynamic output
        self.spec.outputs.dynamic = False
        self.spec.outputs.valid_type = None

        # Now add and check behaviour
        self.spec.outputs.dynamic = True
        self.spec.outputs.valid_type = str
        self.assertTrue(self.spec.validate_outputs({'dummy': 'foo'})[0])
        self.assertTrue(self.spec.validate_outputs({'dummy': StrSubtype('bar')})[0])
        self.assertFalse(self.spec.validate_outputs({'dummy': 5})[0])

    def test_get_description(self):
        spec = ProcessSpec()

        # Adding an input should create some description
        spec.input('test')
        description = spec.get_description()
        self.assertNotEqual(description, {})

        # Similar with adding output
        spec = ProcessSpec()
        spec.output('test')
        description = spec.get_description()
        self.assertNotEqual(description, {})

    def test_input_namespaced(self):
        """
        Test the creation of a namespaced input port
        """
        self.spec.input('some.name.space.a', valid_type=int)

        self.assertTrue('some' in self.spec.inputs)
        self.assertTrue('name' in self.spec.inputs['some'])
        self.assertTrue('space' in self.spec.inputs['some']['name'])
        self.assertTrue('a' in self.spec.inputs['some']['name']['space'])

        self.assertTrue(isinstance(self.spec.inputs.get_port('some'), PortNamespace))
        self.assertTrue(isinstance(self.spec.inputs.get_port('some.name'), PortNamespace))
        self.assertTrue(isinstance(self.spec.inputs.get_port('some.name.space'), PortNamespace))
        self.assertTrue(isinstance(self.spec.inputs.get_port('some.name.space.a'), InputPort))

    def test_validate(self):
        """
        Test the global spec validator functionality.
        """
        def is_valid(spec, inputs):
            if ('a' in inputs) ^ ('b' in inputs):
                return True, None
            else:
                return False, 'Must have a OR b in inputs'

        self.spec.input('a', required=False)
        self.spec.input('b', required=False)
        self.spec.inputs.validator = is_valid

        valid, msg = self.spec.validate_inputs(inputs={})
        self.assertFalse(valid, msg)

        valid, msg = self.spec.validate_inputs(inputs={'a': 'a', 'b': 'b'})
        self.assertFalse(valid, msg)

        valid, msg = self.spec.validate_inputs(inputs={'a': 'a'})
        self.assertTrue(valid, msg)

        valid, msg = self.spec.validate_inputs(inputs={'b': 'b'})
        self.assertTrue(valid, msg)