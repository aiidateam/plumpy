from .utils import TestCase

from plum.port import InputPort, PortNamespace


class TestInputPort(TestCase):

    def test_default(self):
        """
        Test the default value property for the InputPort
        """
        ip = InputPort('test', default=5)
        self.assertEqual(ip.default, 5)

        with self.assertRaises(ValueError):
            InputPort('test', default=4, valid_type=str)


class TestPortNamespace(TestCase):

    BASE_PORT_NAME = 'base'
    BASE_PORT_NAMESPACE_NAME = 'base'

    def setUp(self):
        self.port = InputPort(self.BASE_PORT_NAME)
        self.port_namespace = PortNamespace(self.BASE_PORT_NAMESPACE_NAME)

    def test_port_namespace(self):
        """
        Test basic properties and methods of an empty PortNamespace
        """
        self.assertEqual(self.port_namespace.name, self.BASE_PORT_NAMESPACE_NAME)
        self.assertEqual(len(self.port_namespace), 0)

        with self.assertRaises(ValueError):
            self.port_namespace['key'] = 5

        with self.assertRaises(KeyError):
            self.port_namespace['non_existent']

    def test_port_namespace_add_port(self):
        """
        Test creation of nested namespaces in PortNamespace
        """
        self.port_namespace.add_port(self.port, self.BASE_PORT_NAMESPACE_NAME)
        port = self.port_namespace.get_port(self.BASE_PORT_NAMESPACE_NAME)
        self.assertEqual(port.name, self.BASE_PORT_NAMESPACE_NAME)

        self.port_namespace.add_port(self.port, 'some.nested.namespace.base')

        self.assertTrue('some' in self.port_namespace)
        self.assertTrue('nested' in self.port_namespace['some'])
        self.assertTrue('namespace' in self.port_namespace['some']['nested'])

        self.assertTrue(isinstance(self.port_namespace['some'], PortNamespace))
        self.assertTrue(isinstance(self.port_namespace['some']['nested'], PortNamespace))
        self.assertTrue(isinstance(self.port_namespace['some']['nested']['namespace'], PortNamespace))
        self.assertTrue(isinstance(self.port_namespace['some']['nested']['namespace']['base'], InputPort))

    def test_port_namespace_get_port(self):
        """
        Test get_port for direct and namespaced ports
        """
        with self.assertRaises(KeyError):
            self.port_namespace.get_port(self.BASE_PORT_NAME)

        self.port_namespace.add_port(self.port, self.BASE_PORT_NAME)
        port = self.port_namespace.get_port(self.BASE_PORT_NAME)
        self.assertEqual(port.name, self.BASE_PORT_NAME)

        self.port_namespace.add_port(self.port, 'some.namespace.' + self.BASE_PORT_NAME)
        port = self.port_namespace.get_port('some.namespace.' + self.BASE_PORT_NAME)
        self.assertEqual(port.name, self.BASE_PORT_NAME)

    def test_port_namespace_add_port_namespace(self):
        """
        Test creation of a nested port namespaces within a PortNamespace
        """
        self.port_namespace.add_port_namespace('sub.namespace.bang')
        port_namespace = self.port_namespace.get_port('sub.namespace.bang')

        self.assertEqual(port_namespace.name, 'bang')
        self.assertTrue(isinstance(port_namespace, PortNamespace))

        self.port_namespace.add_port_namespace('test.space', dynamic=True, valid_type=(int))

        # Constructor keyword arguments should only be applied to terminal namespace
        self.assertEqual(self.port_namespace['test'].is_dynamic, False)
        self.assertEqual(self.port_namespace['test']['space'].is_dynamic, True)

        self.assertEqual(self.port_namespace['test'].valid_type, None)
        self.assertEqual(self.port_namespace['test']['space'].valid_type, (int))


        # The add_port_namespace method should return the last created namespace
        port_namespace_bang = self.port_namespace.add_port_namespace('some.tortoise.bang')
        self.assertEqual(port_namespace_bang, self.port_namespace.get_port('some.tortoise.bang'))