from unittest import TestCase
from plum.lang import protected


class A(object):
    def __init__(self):
        self._a = None

    @protected()
    def protected_fn(self):
        return self._a

    @property
    @protected()
    def protected_property(self):
        return self._a

    @protected(check=False)
    def protected_fn_nocheck(self):
        return self._a

    def testA(self):
        self.protected_fn()
        self.protected_property


class B(A):
    def testB(self):
        self.protected_fn()
        self.protected_property


class C(B):
    def testC(self):
        self.protected_fn()
        self.protected_property


class TestProtected(TestCase):
    def test_free_function(self):
        with self.assertRaises(RuntimeError):
            @protected(check=False)
            def some_func():
                pass

    def test_correct_usage(self):
        # All A, B and C should be able to call the protected fn
        A().testA()
        B().testB()
        C().testC()

    def test_incorrect_usage(self):
        # I shouldn't be able to call the protected function from any of them
        a = A()
        with self.assertRaises(RuntimeError):
            a.protected_fn()
        with self.assertRaises(RuntimeError):
            a.protected_property

        b = B()
        with self.assertRaises(RuntimeError):
            b.protected_fn()
        with self.assertRaises(RuntimeError):
            b.protected_property

        c = C()
        with self.assertRaises(RuntimeError):
            c.protected_fn()
        with self.assertRaises(RuntimeError):
            c.protected_property

        with self.assertRaises(RuntimeError):
            class TestWrongDecoratorOrder(object):
                @protected()
                @property
                def a(self):
                    return None


