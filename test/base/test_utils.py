# -*- coding: utf-8 -*-
import unittest
from plumpy.base import utils


class Root:

    @utils.super_check
    def method(self):
        pass

    def do(self):
        utils.call_with_super_check(self.method)


class DoCall(Root):

    def method(self):
        super(DoCall, self).method()


class DontCall(Root):

    def method(self):
        pass


class TestSuperCheckMixin(unittest.TestCase):

    def test_do_call(self):
        DoCall().do()

    def test_dont_call(self):
        with self.assertRaises(AssertionError):
            DontCall().do()

    def dont_call_middle(self):

        class ThirdChild(DontCall):

            def method(self):
                super(ThirdChild, self).method()

        with self.assertRaises(AssertionError):
            ThirdChild.do()

    def test_skip_check_call(self):
        with self.assertRaises(AssertionError):
            DoCall().method()
