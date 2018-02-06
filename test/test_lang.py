from .utils import TestCase
from plumpy.lang import protected, override


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

#
#
# class A(object):
#     def __init__(self):
#         self.a_called = False
#
#     def test(self):
#         self.a_called = True
#
#
# class B(A):
#     def __init__(self):
#         super(B, self).__init__()
#         self.b_called = False
#
#     @call_super
#     def test(self):
#         self.b_called = True
#
#
# class C(B):
#     def __init__(self):
#         super(C, self).__init__()
#         self.c_called = False
#
#     @call_super
#     def test(self):
#         self.c_called = True
#
# class BPrime(A):
#     def __init__(self):
#         super(A, super).__init__()
#         self.b_prime_called = False
#
#     def test(self):
#         self.b_prime_called = True
#
# class CPrime(BPrime):
#     def __init__(self):
#         super(CPrime, self).__init__()
#         self.c_prime_called = False
#
#     @call_super
#     def test(self):
#         self._c_prime_called = True

#  class TestCallSuper(TestCase):
#     def test_one_up(self):
#         b = B()
#         b.test()
#         self.assertTrue(b.a_called)
#         self.assertTrue(b.b_called)
#
#     def test_two_up(self):
#         c = C()
#         c.test()
#         self.assertTrue(c.a_called)
#         self.assertTrue(c.b_called)
#         self.assertTrue(c.c_called)
#
#     def test_two_up_skip_one(self):
#         c_prime = CPrime()
#         c_prime.test()
#         self.assertTrue(c_prime.a_called)
#         self.assertTrue(c_prime.b_prime_called)
#         self.assertFalse(c_prime.a_called)
#
