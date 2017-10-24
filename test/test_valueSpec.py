from util import TestCase
from plum.port import ValueSpec
from plum.exceptions import ValidationError

class TestValueSpec(TestCase):
    def test_required(self):
        s = ValueSpec("required_value", required=True)

        self.assertRaises(ValidationError, s.validate, None)
        s.validate(5)

    def test_validate(self):
        s = ValueSpec("required_value", valid_type=int)

        self.assertRaises(ValidationError, s.validate, 'a')
        s.validate(5)

    def test_validator(self):
        s = ValueSpec("valid_with_validator",
                      validator=lambda x: isinstance(x, int))

        self.assertRaises(ValidationError, s.validate, 's')
        s.validate(5)
