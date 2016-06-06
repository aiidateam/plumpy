from unittest import TestCase
from plum.lang import protected, override


class A(object):
    def __init__(self):
        self._a = None

    @protected(check=True)
    def protected_fn(self):
        return self._a

    @property
    @protected(check=True)
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
                @protected(check=True)
                @property
                def a(self):
                    return None


class Superclass(object):
    def test(self):
        pass


class TestOverride(TestCase):
    def test_free_function(self):
        with self.assertRaises(RuntimeError):
            @override(check=False)
            def some_func():
                pass

    def test_correct_usage(self):
        class Derived(Superclass):
            @override(check=True)
            def test(self):
                return True

        self.assertTrue(Derived().test())

        class Middle(Superclass):
            pass

        class Next(Middle):
            @override(check=True)
            def test(self):
                return True

        self.assertTrue(Next().test())

    def test_incorrect_usage(self):
        class Derived(object):
            @override(check=True)
            def test(self):
                pass

        with self.assertRaises(RuntimeError):
            Derived().test()

        with self.assertRaises(RuntimeError):
            class TestWrongDecoratorOrder(Superclass):
                @override(check=True)
                @property
                def test(self):
                    return None


