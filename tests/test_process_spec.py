# -*- coding: utf-8 -*-
from plumpy import ProcessSpec
from plumpy.ports import InputPort, PortNamespace


class StrSubtype(str):
    pass


class TestProcessSpec:

    def test_get_port_namespace_base(self):
        """
        Get the root, inputs and outputs port namespaces of the ProcessSpec
        """
        spec = ProcessSpec()
        input_ports = spec.inputs
        output_ports = spec.outputs

        assert input_ports.name, spec.NAME_INPUTS_PORT_NAMESPACE
        assert output_ports.name, spec.NAME_OUTPUTS_PORT_NAMESPACE

    def test_dynamic_output(self):
        spec = ProcessSpec()
        spec.outputs.dynamic = True
        spec.outputs.valid_type = str
        assert spec.outputs.validate({'dummy': 'foo'}) is None
        assert spec.outputs.validate({'dummy': StrSubtype('bar')}) is None
        assert spec.outputs.validate({'dummy': 5}) is not None

        # Remove dynamic output
        spec.outputs.dynamic = False
        spec.outputs.valid_type = None

        # Now add and check behaviour
        spec.outputs.dynamic = True
        spec.outputs.valid_type = str
        assert spec.outputs.validate({'dummy': 'foo'}) is None
        assert spec.outputs.validate({'dummy': StrSubtype('bar')}) is None
        assert spec.outputs.validate({'dummy': 5}) is not None

    def test_get_description(self):
        spec = ProcessSpec()

        # Adding an input should create some description
        spec.input('test')
        description = spec.get_description()
        assert description != {}

        # Similar with adding output
        spec = ProcessSpec()
        spec.output('test')
        description = spec.get_description()
        assert description != {}

    def test_input_namespaced(self):
        """
        Test the creation of a namespaced input port
        """
        spec = ProcessSpec()
        spec.input('some.name.space.a', valid_type=int)

        assert 'some' in spec.inputs
        assert 'name' in spec.inputs['some']
        assert 'space' in spec.inputs['some']['name']
        assert 'a' in spec.inputs['some']['name']['space']

        assert isinstance(spec.inputs.get_port('some'), PortNamespace)
        assert isinstance(spec.inputs.get_port('some.name'), PortNamespace)
        assert isinstance(spec.inputs.get_port('some.name.space'), PortNamespace)
        assert isinstance(spec.inputs.get_port('some.name.space.a'), InputPort)

    def test_validator(self):
        """Test the port validator with default."""

        def dict_validator(dictionary, port):
            if 'key' not in dictionary or dictionary['key'] != 'value':
                return 'Invalid dictionary'

        spec = ProcessSpec()
        spec.input('dict', default={'key': 'value'}, validator=dict_validator)

        processed = spec.inputs.pre_process({})
        assert processed == {'dict': {'key': 'value'}}
        spec.inputs.validate()

        processed = spec.inputs.pre_process({'dict': {'key': 'value'}})
        assert processed == {'dict': {'key': 'value'}}
        spec.inputs.validate()

        assert spec.inputs.validate({'dict': {'wrong_key': 'value'}}) is not None

    def test_validate(self):
        """Test the global spec validator functionality."""

        def is_valid(inputs, port):
            if not ('a' in inputs) ^ ('b' in inputs):
                return 'Must have a OR b in inputs'
            return

        spec = ProcessSpec()
        spec.input('a', required=False)
        spec.input('b', required=False)
        spec.inputs.validator = is_valid

        processed = spec.inputs.pre_process({'a': 'a'})
        assert processed == {'a': 'a'}
        spec.inputs.validate()

        processed = spec.inputs.pre_process({'b': 'b'})
        assert processed == {'b': 'b'}
        spec.inputs.validate()

        assert spec.inputs.validate({}) is not None
        assert spec.inputs.validate({'a': 'a', 'b': 'b'}) is not None
