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

        with self.assertRaises(TypeError):
            self.port_namespace['key'] = 5

        with self.assertRaises(KeyError):
            self.port_namespace['non_existent']

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
            port_namespace = self.port_namespace.get_port()

        with self.assertRaises(ValueError):
            port_namespace = self.port_namespace.get_port(5)

        with self.assertRaises(ValueError):
            port_namespace = self.port_namespace.get_port('sub')

        port_namespace_sub = self.port_namespace.create_port_namespace('sub')
        self.assertEqual(self.port_namespace.get_port('sub'), port_namespace_sub)

        with self.assertRaises(ValueError):
            port_namespace = self.port_namespace.get_port('sub.name.space')

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
            port_namespace = self.port_namespace.create_port_namespace()

        with self.assertRaises(ValueError):
            port_namespace = self.port_namespace.create_port_namespace(5)

        port_namespace_sub = self.port_namespace.create_port_namespace('sub')
        port_namespace_sub = self.port_namespace.create_port_namespace('some.nested.sub.space')

        # Existing intermediate nested spaces should be no problem
        port_namespace_sub = self.port_namespace.create_port_namespace('sub.nested.space')

        # Overriding Port is not possible though
        port_namespace_sub[self.BASE_PORT_NAME] = self.port

        with self.assertRaises(ValueError):
            self.port_namespace.create_port_namespace('sub.nested.space.' + self.BASE_PORT_NAME + '.further')