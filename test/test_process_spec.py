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
        self.assertIsNone(self.spec.outputs.validate({'dummy': 'foo'}))
        self.assertIsNone(self.spec.outputs.validate({'dummy': StrSubtype('bar')}))
        self.assertIsNotNone(self.spec.outputs.validate({'dummy': 5}))

        # Remove dynamic output
        self.spec.outputs.dynamic = False
        self.spec.outputs.valid_type = None

        # Now add and check behaviour
        self.spec.outputs.dynamic = True
        self.spec.outputs.valid_type = str
        self.assertIsNone(self.spec.outputs.validate({'dummy': 'foo'}))
        self.assertIsNone(self.spec.outputs.validate({'dummy': StrSubtype('bar')}))
        self.assertIsNotNone(self.spec.outputs.validate({'dummy': 5}))

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

    def test_validator(self):
        """Test the port validator with default."""

        def dict_validator(dictionary, port):
            if 'key' not in dictionary or dictionary['key'] is not 'value':
                return 'Invalid dictionary'

        self.spec.input('dict', default={'key': 'value'}, validator=dict_validator)

        processed = self.spec.inputs.pre_process({})
        self.assertEqual(processed, {'dict': {'key': 'value'}})
        self.spec.inputs.validate()

        processed = self.spec.inputs.pre_process({'dict': {'key': 'value'}})
        self.assertEqual(processed, {'dict': {'key': 'value'}})
        self.spec.inputs.validate()

        self.assertIsNotNone(self.spec.inputs.validate({'dict': {'wrong_key': 'value'}}))

    def test_validate(self):
        """Test the global spec validator functionality."""

        def is_valid(inputs, port):
            if not ('a' in inputs) ^ ('b' in inputs):
                return 'Must have a OR b in inputs'
            return

        self.spec.input('a', required=False)
        self.spec.input('b', required=False)
        self.spec.inputs.validator = is_valid

        processed = self.spec.inputs.pre_process({'a': 'a'})
        self.assertEqual(processed, {'a': 'a'})
        self.spec.inputs.validate()

        processed = self.spec.inputs.pre_process({'b': 'b'})
        self.assertEqual(processed, {'b': 'b'})
        self.spec.inputs.validate()

        self.assertIsNotNone(self.spec.inputs.validate({}))
        self.assertIsNotNone(self.spec.inputs.validate({'a': 'a', 'b': 'b'}))
