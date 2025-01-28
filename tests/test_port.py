# -*- coding: utf-8 -*-
import types

from plumpy.ports import UNSPECIFIED, InputPort, OutputPort, Port, PortNamespace
import pytest


class TestPort:
    def test_required(self):
        spec = Port('required_value', required=True)

        assert spec.validate(UNSPECIFIED) is not None
        assert spec.validate(5) is None

    def test_validate(self):
        spec = Port('required_value', valid_type=int)

        assert spec.validate(5) is None
        assert spec.validate('a') is not None

    def test_validator(self):
        def validate(value, port):
            assert isinstance(port, Port)
            if not isinstance(value, int):
                return 'Not int'
            return None

        spec = Port('valid_with_validator', validator=validate)

        assert spec.validate(5) is None
        assert spec.validate('s') is not None

    def test_validator_not_required(self):
        """Verify that a validator is not called if no value is specified for a port that is not required."""

        def validate(value, port):
            raise RuntimeError

        spec = Port('valid_with_validator', validator=validate, required=False)

        assert spec.validate(UNSPECIFIED) is None


class TestInputPort:
    def test_default(self):
        """Test the default value property for the InputPort."""
        port = InputPort('test', default=5)
        assert port.default == 5

        with pytest.raises(ValueError):
            InputPort('test', default=4, valid_type=str)

    def test_validator(self):
        """Test the validator functionality."""

        def integer_validator(value, port):
            assert isinstance(port, Port)
            if value < 0:
                return 'Only positive integers allowed'

        port = InputPort('test', validator=integer_validator)
        assert port.validate(5) is None
        assert port.validate(-5) is not None

    def test_lambda_default(self):
        """Test a default with a lambda."""
        from plumpy.ports import UNSPECIFIED

        # The default should not be evaluated upon construction, so even if it will return an incorrect type, the
        # following call should not fail
        InputPort('lambda_default', default=lambda: 'string', valid_type=int)

        port = InputPort('test', default=lambda: 5)

        assert port.validate(UNSPECIFIED) is None
        assert port.validate(3) is None

        # Testing that passing an actual lambda as a value is alos possible
        port = InputPort('test', valid_type=(types.FunctionType, int), default=lambda: 5)

        def some_lambda():
            return 'string'

        assert port.validate(some_lambda) is None


class TestOutputPort:
    def test_default(self):
        """
        Test the default value property for the InputPort
        """
        name = 'output'
        valid_type = int
        help_string = 'Help string'
        required = False

        def validator(value, port):
            assert isinstance(port, Port)

        port = OutputPort(name, valid_type=valid_type, help=help_string, required=required, validator=validator)
        assert port.name == name
        assert port.valid_type == valid_type
        assert port.help == help_string
        assert port.required == required
        assert port.validator == validator


class TestPortNamespace:
    BASE_PORT_NAME = 'port'
    BASE_PORT_NAMESPACE_NAME = 'port'

    def test_port_namespace(self):
        """
        Test basic properties and methods of an empty PortNamespace
        """
        port_namespace = PortNamespace(self.BASE_PORT_NAMESPACE_NAME)
        assert port_namespace.name == self.BASE_PORT_NAMESPACE_NAME
        assert len(port_namespace) == 0

        with pytest.raises(TypeError):
            port_namespace['key'] = 5

        with pytest.raises(KeyError):
            port_namespace['non_existent']

    def test_port_namespace_valid_type_and_dynamic(self):
        """Test that `dynamic` and `valid_type` attributes defined through constructor are properly set."""

        # Setting `dynamic=True` should leave `valid_type` untouched
        port_namespace = PortNamespace(dynamic=True)
        assert port_namespace.valid_type == None
        assert port_namespace.dynamic == True

        # Setting `valid_type` to not `None` should automatically set `dynamic=True`
        port_namespace = PortNamespace(valid_type=int)
        assert port_namespace.valid_type == int
        assert port_namespace.dynamic == True

        # The following does not make sense, but the constructor cannot raise a warning because it cannot detect whether
        # the `dynamic=False` is explicitly set by the user or is merely the default. In any case, the `dynamic=False`
        # is simply ignored in this case
        port_namespace = PortNamespace(dynamic=False, valid_type=int)
        assert port_namespace.valid_type == int
        assert port_namespace.dynamic == True

    def test_port_namespace_validation(self):
        """Test validate method of a `PortNamespace`."""

        def validator(port_values, port):
            assert isinstance(port, PortNamespace)
            if port_values['explicit'] < 0 or port_values['dynamic'] < 0:
                return 'Only positive integers allowed'

        port_namespace = PortNamespace(self.BASE_PORT_NAMESPACE_NAME)
        port_namespace['explicit'] = InputPort('explicit', valid_type=int)
        port_namespace.validator = validator
        port_namespace.valid_type = int

        # The explicit ports will be validated first before the namespace validator is called.
        assert port_namespace.validate({'explicit': 1, 'dynamic': 5}) is None
        assert port_namespace.validate({'dynamic': -5}) is not None

        # Validator should not be called if the namespace is not required and no value is specified for the namespace
        port_namespace.required = False
        assert port_namespace.validate() is None

    def test_port_namespace_dynamic(self):
        """
        Setting a valid type for a PortNamespace should automatically make it dynamic
        """
        port_namespace = PortNamespace(self.BASE_PORT_NAMESPACE_NAME)
        assert not port_namespace.dynamic

        port_namespace.valid_type = (str, int)

        assert port_namespace.dynamic
        assert port_namespace.valid_type == (str, int)

    def test_port_namespace_get_port(self):
        """
        Test get_port of PortNamespace will retrieve nested PortNamespaces and Ports as long
        as they and all intermediate nested PortNamespaces exist
        """
        port = InputPort(self.BASE_PORT_NAME)
        port_namespace = PortNamespace(self.BASE_PORT_NAMESPACE_NAME)
        with pytest.raises(TypeError):
            port_namespace.get_port()

        with pytest.raises(ValueError):
            port_namespace.get_port(5)

        with pytest.raises(ValueError):
            port_namespace.get_port('sub')

        port_namespace_sub = port_namespace.create_port_namespace('sub')
        assert port_namespace.get_port('sub') == port_namespace_sub

        with pytest.raises(ValueError):
            port_namespace.get_port('sub.name.space')

        port_namespace_sub = port_namespace.create_port_namespace('sub.name.space')
        assert port_namespace.get_port('sub.name.space') == port_namespace_sub

        # Add Port into subnamespace and try to get it in one go from top level port namespace
        port_namespace_sub[self.BASE_PORT_NAME] = port
        assert port_namespace.get_port('sub.name.space.' + self.BASE_PORT_NAME) == port

    def test_port_namespace_get_port_dynamic(self):
        """Test ``get_port`` with the ``create_dynamically=True`` keyword.

        In this case, the method should create the subnamespace on-the-fly with the same stats as the host namespace.
        """
        port_namespace = PortNamespace(self.BASE_PORT_NAMESPACE_NAME, dynamic=True)

        name = 'undefined'
        sub_namespace = port_namespace.get_port(name, create_dynamically=True)

        assert isinstance(sub_namespace, PortNamespace)
        assert sub_namespace.dynamic
        assert sub_namespace.name == name

        name = 'nested.undefined'
        sub_namespace = port_namespace.get_port(name, create_dynamically=True)

        assert isinstance(sub_namespace, PortNamespace)
        assert sub_namespace.dynamic
        assert sub_namespace.name == 'undefined'

    def test_port_namespace_create_port_namespace(self):
        """
        Test the create_port_namespace function of the PortNamespace class
        """
        port = InputPort(self.BASE_PORT_NAME)
        port_namespace = PortNamespace(self.BASE_PORT_NAMESPACE_NAME)
        with pytest.raises(TypeError):
            port_namespace.create_port_namespace()

        with pytest.raises(ValueError):
            port_namespace.create_port_namespace(5)

        port_namespace_sub = port_namespace.create_port_namespace('sub')
        port_namespace_sub = port_namespace.create_port_namespace('some.nested.sub.space')

        # Existing intermediate nested spaces should be no problem
        port_namespace_sub = port_namespace.create_port_namespace('sub.nested.space')

        # Overriding Port is not possible though
        port_namespace_sub[self.BASE_PORT_NAME] = port

        with pytest.raises(ValueError):
            port_namespace.create_port_namespace('sub.nested.space.' + self.BASE_PORT_NAME + '.further')

    def test_port_namespace_set_valid_type(self):
        """Setting a valid type for a PortNamespace should automatically mark it as dynamic."""
        port_namespace = PortNamespace(self.BASE_PORT_NAMESPACE_NAME)
        assert not port_namespace.dynamic
        assert port_namespace.valid_type is None

        # Setting the `valid_type` should automatically set `dynamic=True` because it does not make sense to define a
        # a specific type but then not allow any values whatsoever.
        port_namespace.valid_type = int

        assert port_namespace.dynamic
        assert port_namespace.valid_type == int

        port_namespace.valid_type = None

        # Setting `valid_type` to `None` however does not automatically revert the `dynamic` attribute
        assert port_namespace.dynamic
        assert port_namespace.valid_type is None

    def test_port_namespace_validate(self):
        """Check that validating of sub namespaces works correctly.

        By setting a valid type on a port namespace, it automatically becomes dynamic. Port namespaces that are dynamic
        should accept arbitrarily nested input and should validate, as long as all leaf values satisfy the `valid_type`.
        """
        port_namespace = PortNamespace(self.BASE_PORT_NAMESPACE_NAME)
        port_namespace_sub = port_namespace.create_port_namespace('sub.space')
        port_namespace_sub.valid_type = int
        assert port_namespace_sub.dynamic

        # Check that passing a non mapping type raises
        validation_error = port_namespace.validate(5)
        assert validation_error is not None

        # Valid input
        validation_error = port_namespace.validate({'sub': {'space': {'output': 5}}})
        assert validation_error is None

        # Valid input: `sub.space` is dynamic, so should allow arbitrarily nested namespaces as long as the leaf values
        # match the valid type, which is `int` in this example.
        validation_error = port_namespace.validate({'sub': {'space': {'output': {'invalid': 5}}}})
        assert validation_error is None

        # Invalid input - the value in ``space`` is not ``int`` but a ``str``
        validation_error = port_namespace.validate({'sub': {'space': {'output': '5'}}})
        assert validation_error is not None

        # Check the breadcrumbs are correct
        assert validation_error.port == port_namespace.NAMESPACE_SEPARATOR.join(
            (self.BASE_PORT_NAMESPACE_NAME, 'sub', 'space', 'output')
        )

    def test_port_namespace_required(self):
        """Verify that validation will fail if required port is not specified."""
        port_namespace = PortNamespace(self.BASE_PORT_NAMESPACE_NAME)
        port_namespace_sub = port_namespace.create_port_namespace('sub.space')
        port_namespace_sub.valid_type = int

        # Create a required port
        port_namespace['required_port'] = OutputPort('required_port', valid_type=int, required=True)

        # No port values at all should fail
        port_values = {}
        validation_error = port_namespace.validate(port_values)
        assert validation_error is not None

        # Some port value, but still the required output is not defined, so should fail
        port_values = {'sub': {'space': {'output': 5}}}
        validation_error = port_namespace.validate(port_values)
        assert validation_error is not None

        # Specifying the required port and some additional ones should be valid
        port_values = {'sub': {'space': {'output': 5}}, 'required_port': 1}
        validation_error = port_namespace.validate(port_values)
        assert validation_error is None

        # Also just the required port should be valid
        port_values = {'required_port': 1}
        validation_error = port_namespace.validate(port_values)
        assert validation_error is None

    def test_port_namespace_no_populate_defaults(self):
        """Verify that defaults are not populated for a `populate_defaults=False` namespace in `pre_process`."""
        port_namespace = PortNamespace('base')
        port_namespace_normal = port_namespace.create_port_namespace('normal', populate_defaults=True)
        port_namespace_normal['with_default'] = InputPort('with_default', default=1, valid_type=int)
        port_namespace_normal['without_default'] = InputPort('without_default', valid_type=int)

        inputs = {}
        pre_processed = port_namespace.pre_process(inputs)
        assert 'normal' in pre_processed
        assert 'with_default' in pre_processed.normal
        assert 'without_default' not in pre_processed.normal

        # Now repeat the test but with a "lazy" namespace where defaults are not populated if not explicitly specified
        port_namespace = PortNamespace('base')
        port_namespace_lazy = port_namespace.create_port_namespace('lazy', populate_defaults=False, required=False)
        port_namespace_lazy['with_default'] = InputPort('with_default', default=1, valid_type=int)
        port_namespace_lazy['without_default'] = InputPort('without_default', valid_type=int)

        inputs = {}
        pre_processed = port_namespace.pre_process(inputs)

        # Because the namespace is lazy and no inputs were passed, the defaults should not have been populated.
        assert pre_processed == {}

    def test_port_namespace_lambda_defaults(self):
        """Verify that lambda defaults are accepted and properly evaluated."""
        port_namespace = PortNamespace('base')
        port_namespace['lambda_default'] = InputPort(
            'lambda_default', default=lambda: 1, valid_type=(types.FunctionType, int)
        )

        inputs = port_namespace.pre_process({})
        assert inputs['lambda_default'] == 1
        assert port_namespace.validate(inputs) is None

        inputs = port_namespace.pre_process({'lambda_default': 5})
        assert inputs['lambda_default'] == 5
        assert port_namespace.validate(inputs) is None

        # When passing a lambda directly as the value, it should NOT be evaluated during pre_processing
        def some_lambda():
            return 5

        inputs = port_namespace.pre_process({'lambda_default': some_lambda})
        assert inputs['lambda_default'] == some_lambda
        assert port_namespace.validate(inputs) is None
