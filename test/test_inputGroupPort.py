from past.builtins import basestring 

from .utils import TestCase
from plum.port import InputGroupPort


class TestInputGroupPort(TestCase):
    def test_validate(self):
        p = InputGroupPort("test")
        self.assertTrue(p.validate({})[0])

        p = InputGroupPort("test", required=True)
        self.assertFalse(p.validate(None)[0])
        self.assertTrue(p.validate({})[0])
        self.assertTrue(p.validate({'a': 'value'})[0])

        p = InputGroupPort("test", default={})
        self.assertTrue(p.validate(None)[0])
        self.assertTrue(p.validate({})[0])

        p = InputGroupPort("test", valid_type=basestring)
        self.assertTrue(p.validate({'a': 'value'})[0])

        p = InputGroupPort("test", required=True, valid_type=(basestring, int))
        self.assertTrue(p.validate({'a': 'value', 'b': 3}))
