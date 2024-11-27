# -*- coding: utf-8 -*-
import types

from plumpy.ports import UNSPECIFIED, InputPort, OutputPort, Port, PortNamespace

from .utils import TestCase


class TestPort(TestCase):
    def test_required(self):
        spec = Port('required_value', required=True)

        self.assertIsNotNone(spec.validate(UNSPECIFIED))
        self.assertIsNone(spec.validate(5))

    def test_validate(self):
        spec = Port('required_value', valid_type=int)

        self.assertIsNone(spec.validate(5))
        self.assertIsNotNone(spec.validate('a'))

    def test_validator(self):
        def validate(value, port):
            assert isinstance(port, Port)
            if not isinstance(value, int):
                return 'Not int'
            return None

        spec = Port('valid_with_validator', validator=validate)

        self.assertIsNone(spec.validate(5))
        self.assertIsNotNone(spec.validate('s'))

    def test_validator_not_required(self):
        """Verify that a validator is not called if no value is specified for a port that is not required."""

        def validate(value, port):
            raise RuntimeError

        spec = Port('valid_with_validator', validator=validate, required=False)

        self.assertIsNone(spec.validate(UNSPECIFIED))


class TestInputPort(TestCase):
    def test_default(self):
        """Test the default value property for the InputPort."""
        port = InputPort('test', default=5)
        self.assertEqual(port.default, 5)

        with self.assertRaises(ValueError):
            InputPort('test', default=4, valid_type=str)

    def test_validator(self):
        """Test the validator functionality."""

        def integer_validator(value, port):
            assert isinstance(port, Port)
            if value < 0:
                return 'Only positive integers allowed'

        port = InputPort('test', validator=integer_validator)
        self.assertIsNone(port.validate(5))
        self.assertIsNotNone(port.validate(-5))

    def test_lambda_default(self):
        """Test a default with a lambda."""
        from plumpy.ports import UNSPECIFIED

        # The default should not be evaluated upon construction, so even if it will return an incorrect type, the
        # following call should not fail
        InputPort('lambda_default', default=lambda: 'string', valid_type=int)

        port = InputPort('test', default=lambda: 5)

        self.assertIsNone(port.validate(UNSPECIFIED))
        self.assertIsNone(port.validate(3))

        # Testing that passing an actual lambda as a value is alos possible
        port = InputPort('test', valid_type=(types.FunctionType, int), default=lambda: 5)

        def some_lambda():
            "string"

        self.assertIsNone(port.validate(some_lambda))


class TestOutputPort(TestCase):
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
        self.assertEqual(port.name, name)
        self.assertEqual(port.valid_type, valid_type)
        self.assertEqual(port.help, help_string)
        self.assertEqual(port.required, required)
        self.assertEqual(port.validator, validator)


class TestPortNamespace(TestCase):
    BASE_PORT_NAME = 'port'
    BASE_PORT_NAMESPACE_NAME = 'port'

    def setUp(self):
        self.port = InputPort(self.BASE_PORT_NAME)
        self.port_namespace = PortNamespace(self.BASE_PORT_NAMESPACE_NAME)

    def test_port_namespace(self):
        """
        Test basic properties and methods of an empty PortNamespace
        """
        self.assertEqual(self.port_namespace.name, self.BASE_PORT_NAMESPACE_NAME)
        self.assertEqual(len(self.port_namespace), 0)

        with self.assertRaises(TypeError):
            self.port_namespace['key'] = 5

        with self.assertRaises(KeyError):
            self.port_namespace['non_existent']

    def test_port_namespace_valid_type_and_dynamic(self):
        """Test that `dynamic` and `valid_type` attributes defined through constructor are properly set."""

        # Setting `dynamic=True` should leave `valid_type` untouched
        port_namespace = PortNamespace(dynamic=True)
        self.assertEqual(port_namespace.valid_type, None)
        self.assertEqual(port_namespace.dynamic, True)

        # Setting `valid_type` to not `None` should automatically set `dynamic=True`
        port_namespace = PortNamespace(valid_type=int)
        self.assertEqual(port_namespace.valid_type, int)
        self.assertEqual(port_namespace.dynamic, True)

        # The following does not make sense, but the constructor cannot raise a warning because it cannot detect whether
        # the `dynamic=False` is explicitly set by the user or is merely the default. In any case, the `dynamic=False`
        # is simply ignored in this case
        port_namespace = PortNamespace(dynamic=False, valid_type=int)
        self.assertEqual(port_namespace.valid_type, int)
        self.assertEqual(port_namespace.dynamic, True)

    def test_port_namespace_validation(self):
        """Test validate method of a `PortNamespace`."""

        def validator(port_values, port):
            assert isinstance(port, PortNamespace)
            if port_values['explicit'] < 0 or port_values['dynamic'] < 0:
                return 'Only positive integers allowed'

        self.port_namespace['explicit'] = InputPort('explicit', valid_type=int)
        self.port_namespace.validator = validator
        self.port_namespace.valid_type = int

        # The explicit ports will be validated first before the namespace validator is called.
        self.assertIsNone(self.port_namespace.validate({'explicit': 1, 'dynamic': 5}))
        self.assertIsNotNone(self.port_namespace.validate({'dynamic': -5}))

        # Validator should not be called if the namespace is not required and no value is specified for the namespace
        self.port_namespace.required = False
        self.assertIsNone(self.port_namespace.validate())

    def test_port_namespace_dynamic(self):
        """
        Setting a valid type for a PortNamespace should automatically make it dynamic
        """
        self.assertFalse(self.port_namespace.dynamic)

        self.port_namespace.valid_type = (str, int)

        self.assertTrue(self.port_namespace.dynamic)
        self.assertEqual(self.port_namespace.valid_type, (str, int))

    def test_port_namespace_get_port(self):
        """
        Test get_port of PortNamespace will retrieve nested PortNamespaces and Ports as long
        as they and all intermediate nested PortNamespaces exist
        """
        with self.assertRaises(TypeError):
            self.port_namespace.get_port()

        with self.assertRaises(ValueError):
            self.port_namespace.get_port(5)

        with self.assertRaises(ValueError):
            self.port_namespace.get_port('sub')

        port_namespace_sub = self.port_namespace.create_port_namespace('sub')
        self.assertEqual(self.port_namespace.get_port('sub'), port_namespace_sub)

        with self.assertRaises(ValueError):
            self.port_namespace.get_port('sub.name.space')

        port_namespace_sub = self.port_namespace.create_port_namespace('sub.name.space')
        self.assertEqual(self.port_namespace.get_port('sub.name.space'), port_namespace_sub)

        # Add Port into subnamespace and try to get it in one go from top level port namespace
        port_namespace_sub[self.BASE_PORT_NAME] = self.port
        port = self.port_namespace.get_port('sub.name.space.' + self.BASE_PORT_NAME)
        self.assertEqual(port, self.port)

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
        with self.assertRaises(TypeError):
            self.port_namespace.create_port_namespace()

        with self.assertRaises(ValueError):
            self.port_namespace.create_port_namespace(5)

        port_namespace_sub = self.port_namespace.create_port_namespace('sub')
        port_namespace_sub = self.port_namespace.create_port_namespace('some.nested.sub.space')

        # Existing intermediate nested spaces should be no problem
        port_namespace_sub = self.port_namespace.create_port_namespace('sub.nested.space')

        # Overriding Port is not possible though
        port_namespace_sub[self.BASE_PORT_NAME] = self.port

        with self.assertRaises(ValueError):
            self.port_namespace.create_port_namespace('sub.nested.space.' + self.BASE_PORT_NAME + '.further')

    def test_port_namespace_set_valid_type(self):
        """Setting a valid type for a PortNamespace should automatically mark it as dynamic."""
        self.assertFalse(self.port_namespace.dynamic)
        self.assertIsNone(self.port_namespace.valid_type)

        # Setting the `valid_type` should automatically set `dynamic=True` because it does not make sense to define a
        # a specific type but then not allow any values whatsoever.
        self.port_namespace.valid_type = int

        self.assertTrue(self.port_namespace.dynamic)
        self.assertEqual(self.port_namespace.valid_type, int)

        self.port_namespace.valid_type = None

        # Setting `valid_type` to `None` however does not automatically revert the `dynamic` attribute
        self.assertTrue(self.port_namespace.dynamic)
        self.assertIsNone(self.port_namespace.valid_type)

    def test_port_namespace_validate(self):
        """Check that validating of sub namespaces works correctly.

        By setting a valid type on a port namespace, it automatically becomes dynamic. Port namespaces that are dynamic
        should accept arbitrarily nested input and should validate, as long as all leaf values satisfy the `valid_type`.
        """
        port_namespace_sub = self.port_namespace.create_port_namespace('sub.space')
        port_namespace_sub.valid_type = int
        assert port_namespace_sub.dynamic

        # Check that passing a non mapping type raises
        validation_error = self.port_namespace.validate(5)
        self.assertIsNotNone(validation_error)

        # Valid input
        validation_error = self.port_namespace.validate({'sub': {'space': {'output': 5}}})
        self.assertIsNone(validation_error)

        # Valid input: `sub.space` is dynamic, so should allow arbitrarily nested namespaces as long as the leaf values
        # match the valid type, which is `int` in this example.
        validation_error = self.port_namespace.validate({'sub': {'space': {'output': {'invalid': 5}}}})
        self.assertIsNone(validation_error)

        # Invalid input - the value in ``space`` is not ``int`` but a ``str``
        validation_error = self.port_namespace.validate({'sub': {'space': {'output': '5'}}})
        self.assertIsNotNone(validation_error)

        # Check the breadcrumbs are correct
        self.assertEqual(
            validation_error.port,
            self.port_namespace.NAMESPACE_SEPARATOR.join((self.BASE_PORT_NAMESPACE_NAME, 'sub', 'space', 'output')),
        )

    def test_port_namespace_required(self):
        """Verify that validation will fail if required port is not specified."""
        port_namespace_sub = self.port_namespace.create_port_namespace('sub.space')
        port_namespace_sub.valid_type = int

        # Create a required port
        self.port_namespace['required_port'] = OutputPort('required_port', valid_type=int, required=True)

        # No port values at all should fail
        port_values = {}
        validation_error = self.port_namespace.validate(port_values)
        self.assertIsNotNone(validation_error)

        # Some port value, but still the required output is not defined, so should fail
        port_values = {'sub': {'space': {'output': 5}}}
        validation_error = self.port_namespace.validate(port_values)
        self.assertIsNotNone(validation_error)

        # Specifying the required port and some additional ones should be valid
        port_values = {'sub': {'space': {'output': 5}}, 'required_port': 1}
        validation_error = self.port_namespace.validate(port_values)
        self.assertIsNone(validation_error)

        # Also just the required port should be valid
        port_values = {'required_port': 1}
        validation_error = self.port_namespace.validate(port_values)
        self.assertIsNone(validation_error)

    def test_port_namespace_no_populate_defaults(self):
        """Verify that defaults are not populated for a `populate_defaults=False` namespace in `pre_process`."""
        port_namespace = PortNamespace('base')
        port_namespace_normal = port_namespace.create_port_namespace('normal', populate_defaults=True)
        port_namespace_normal['with_default'] = InputPort('with_default', default=1, valid_type=int)
        port_namespace_normal['without_default'] = InputPort('without_default', valid_type=int)

        inputs = {}
        pre_processed = port_namespace.pre_process(inputs)
        self.assertIn('normal', pre_processed)
        self.assertIn('with_default', pre_processed.normal)
        self.assertNotIn('without_default', pre_processed.normal)

        # Now repeat the test but with a "lazy" namespace where defaults are not populated if not explicitly specified
        port_namespace = PortNamespace('base')
        port_namespace_lazy = port_namespace.create_port_namespace('lazy', populate_defaults=False, required=False)
        port_namespace_lazy['with_default'] = InputPort('with_default', default=1, valid_type=int)
        port_namespace_lazy['without_default'] = InputPort('without_default', valid_type=int)

        inputs = {}
        pre_processed = port_namespace.pre_process(inputs)

        # Because the namespace is lazy and no inputs were passed, the defaults should not have been populated.
        self.assertEqual(pre_processed, {})

    def test_port_namespace_lambda_defaults(self):
        """Verify that lambda defaults are accepted and properly evaluated."""
        port_namespace = PortNamespace('base')
        port_namespace['lambda_default'] = InputPort(
            'lambda_default', default=lambda: 1, valid_type=(types.FunctionType, int)
        )

        inputs = port_namespace.pre_process({})
        self.assertEqual(inputs['lambda_default'], 1)
        self.assertIsNone(port_namespace.validate(inputs))

        inputs = port_namespace.pre_process({'lambda_default': 5})
        self.assertEqual(inputs['lambda_default'], 5)
        self.assertIsNone(port_namespace.validate(inputs))

        # When passing a lambda directly as the value, it should NOT be evaluated during pre_processing
        def some_lambda():
            return 5

        inputs = port_namespace.pre_process({'lambda_default': some_lambda})
        self.assertEqual(inputs['lambda_default'], some_lambda)
        self.assertIsNone(port_namespace.validate(inputs))
