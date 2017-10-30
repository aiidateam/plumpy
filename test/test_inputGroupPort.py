from past.builtins import basestring 

from .util import TestCase
from plum.port import InputGroupPort
from plum.exceptions import ValidationError


class TestInputGroupPort(TestCase):
    def test_validate(self):
        p = InputGroupPort("test")
        p.validate({})

        p = InputGroupPort("test", required=True)
        self.assertRaises(ValidationError, p.validate, None)
        p.validate({})
        p.validate({'a': 'value'})

        p = InputGroupPort("test", default={})
        p.validate(None)
        p.validate({})

        p = InputGroupPort("test", valid_type=basestring)
        p.validate({'a': 'value'})

        p = InputGroupPort("test", required=True, valid_type=(basestring, int))
        p.validate({'a': 'value', 'b': 3})
