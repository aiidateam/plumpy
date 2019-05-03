from __future__ import absolute_import
from .utils import TestCase

from plumpy.ports import InputPort, OutputPort, PortNamespace


class TestInputPort(TestCase):

    def test_default(self):
        """
        Test the default value property for the InputPort
        """
        ip = InputPort('test', default=5)
        self.assertEqual(ip.default, 5)

        with self.assertRaises(ValueError):
            InputPort('test', default=4, valid_type=str)


class TestOutputPort(TestCase):

    def test_default(self):
        """
        Test the default value property for the InputPort
        """
        name = 'output'
        valid_type = int
        help_string = 'Help string'
        required = False

        def validator(value):
            pass

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

    def test_port_namespace_validation(self):
        """Test validate method of a `PortNamespace`."""
        def validator(port_values):
            if port_values['integer'] < 0:
                return 'Only positive integers allowed'

        self.port_namespace.validator = validator
        self.port_namespace.valid_type = int

        self.assertIsNone(self.port_namespace.validate({'integer': 5}))
        self.assertIsNotNone(self.port_namespace.validate({'integer': -5}))

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
        """
        Setting a valid type for a PortNamespace should automatically mark it as dynamic. Conversely, setting
        the valid_type equal to None should revert dynamic to False
        """
        self.assertFalse(self.port_namespace.dynamic)
        self.assertIsNone(self.port_namespace.valid_type)

        self.port_namespace.valid_type = int

        self.assertTrue(self.port_namespace.dynamic)
        self.assertEqual(self.port_namespace.valid_type, int)

        self.port_namespace.valid_type = None

        self.assertFalse(self.port_namespace.dynamic)
        self.assertIsNone(self.port_namespace.valid_type)

    def test_port_namespace_validate(self):
        """Check that validating of sub namespaces works correctly"""
        port_namespace_sub = self.port_namespace.create_port_namespace('sub.space')
        port_namespace_sub.valid_type = int

        # Check that passing a non mapping type raises
        validation_error = self.port_namespace.validate(5)
        self.assertIsNotNone(validation_error)

        # Valid input
        validation_error = self.port_namespace.validate({'sub': {'space': {'output': 5}}})
        self.assertIsNone(validation_error)

        # Invalid input
        validation_error = self.port_namespace.validate({'sub': {'space': {'output': '5'}}})
        self.assertIsNotNone(validation_error)

        # Check the breadcrumbs are correct
        self.assertEqual(
            validation_error.port,
            self.port_namespace.NAMESPACE_SEPARATOR.join((self.BASE_PORT_NAMESPACE_NAME, 'sub', 'space', 'output')))

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
