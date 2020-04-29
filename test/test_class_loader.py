# -*- coding: utf-8 -*-
import plumpy
import unittest


class MyCls:
    pass


class TestDefaultObjectLoader(unittest.TestCase):

    def test_simple_load(self):
        loader = plumpy.DefaultObjectLoader()
        identifier = loader.identify_object(MyCls)
        cls = loader.load_object(identifier)
        self.assertIs(MyCls, cls)

    def test_custom_loader(self):

        class CustomClassLoader(plumpy.ObjectLoader):

            def identify_object(self, obj):
                if obj is MyCls:
                    return 'MyCls'

            def load_object(self, identifier):
                if identifier == 'MyCls':
                    return MyCls

        loader = CustomClassLoader()
        cls = loader.load_object(loader.identify_object(MyCls))
        self.assertIs(MyCls, cls)
