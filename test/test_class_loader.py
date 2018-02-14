import plumpy
import unittest


class MyCls(object):
    pass


class TestClassLoader(unittest.TestCase):
    def test_simple_load(self):
        cl = plumpy.ClassLoader()
        identifier = cl.class_identifier(MyCls)
        cls = cl.load_class(identifier)
        self.assertIs(MyCls, cls)

    def test_custom_class_loader(self):
        class CustomClassLoader(plumpy.ClassLoader):
            def load_class(self, identifier):
                if identifier == 'MyCls':
                    return MyCls

        cl = CustomClassLoader()
        cls = cl.load_class('MyCls')
        self.assertIs(MyCls, cls)
