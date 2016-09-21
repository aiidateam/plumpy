from unittest import TestCase
from plum.port import ValueSpec


class TestValueSpec(TestCase):
    def test_required(self):
        s = ValueSpec("required_value", required=True)

        self.assertFalse(s.validate(None)[0])
        self.assertTrue(s.validate(5)[0])

    def test_validate(self):
        s = ValueSpec("required_value", valid_type=int)

        self.assertTrue(s.validate(5)[0])
        self.assertFalse(s.validate('a')[0])

    def test_validator(self):
        s = ValueSpec("valid_with_validator",
                      validator=lambda x: isinstance(x, int))

        self.assertTrue(s.validate(5)[0])
        self.assertFalse(s.validate('s')[0])
