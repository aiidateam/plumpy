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

        with self.assertRaises(ValueError):
            self.port_namespace['key'] = 5

        with self.assertRaises(KeyError):
            self.port_namespace['non_existent']

    def test_port_namespace_get_port(self):
        """
        Test get_port of PortNamespace will create nested namespaces as needed
        """
        port_namespace = self.port_namespace.get_port()
        self.assertEqual(port_namespace, self.port_namespace)

        port_namespace = self.port_namespace.get_port('sub')
        self.assertEqual(port_namespace.name, 'sub')
        self.assertTrue(isinstance(port_namespace, PortNamespace))

        port_namespace = self.port_namespace.get_port('sub.name.space')
        self.assertEqual(port_namespace.name, 'space')
        self.assertTrue(isinstance(port_namespace, PortNamespace))

    def test_port_namespace_get_port_namespaced_port(self):
        """
        Test get_port of PortNamespace will properly retrieve a namespaces Port
        """
        # Store a Port instance in BASE_PORT_NAME
        port_namespace = self.port_namespace.get_port()
        port_namespace[self.BASE_PORT_NAME] = self.port

        # Retrieve port from BASE_PORT_NAME using get_port() and check it is the same
        port = self.port_namespace.get_port(self.BASE_PORT_NAME)
        self.assertTrue(isinstance(port, InputPort))
        self.assertEqual(port.name, self.BASE_PORT_NAME)

        # Store a Port instance in some namespace
        port_namespace = self.port_namespace.get_port('sub.name.space')
        port_namespace[self.BASE_PORT_NAME] = self.port

        # Retrieve port from the namespace using get_port() and check it is the same
        port = self.port_namespace.get_port('sub.name.space.' + self.BASE_PORT_NAME)
        self.assertTrue(isinstance(port, InputPort))
        self.assertEqual(port.name, self.BASE_PORT_NAME)