from unittest import TestCase
from plum.port import InputGroupPort


class TestInputGroupPort(TestCase):
    def test_validate(self):
        p = InputGroupPort(None, "test")
        self.assertTrue(p.validate({})[0])

        p = InputGroupPort(None, "test", required=True)
        self.assertFalse(p.validate(None)[0])
        self.assertTrue(p.validate({})[0])
        self.assertTrue(p.validate({'a': 'value'})[0])

        p = InputGroupPort(None, "test", default={})
        self.assertTrue(p.validate(None)[0])
        self.assertTrue(p.validate({})[0])

        p = InputGroupPort(None, "test", valid_type=basestring)
        self.assertTrue(p.validate({'a': 'value'})[0])

        p = InputGroupPort(None, "test", required=True, valid_type=(basestring, int))
        self.assertTrue(p.validate({'a': 'value', 'b': 3}))
