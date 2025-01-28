# -*- coding: utf-8 -*-

from plumpy.base import utils
import pytest


class Root:
    @utils.super_check
    def method(self):
        pass

    def do(self):
        utils.call_with_super_check(self.method)


class DoCall(Root):
    def method(self):
        super().method()


class DontCall(Root):
    def method(self):
        pass


class TestSuperCheckMixin:
    def test_do_call(self):
        DoCall().do()

    def test_dont_call(self):
        with pytest.raises(AssertionError):
            DontCall().do()

    def dont_call_middle(self):
        class ThirdChild(DontCall):
            def method(self):
                super().method()

        with pytest.raises(AssertionError):
            ThirdChild.do()

    def test_skip_check_call(self):
        with pytest.raises(AssertionError):
            DoCall().method()
