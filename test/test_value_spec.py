from __future__ import absolute_import
import plumpy

from . import utils


class TestValueSpec(utils.TestCase):

    def test_required(self):
        spec = plumpy.ValueSpec("required_value", required=True)

        self.assertIsNotNone(spec.validate(plumpy.UNSPECIFIED))
        self.assertIsNone(spec.validate(5))

    def test_validate(self):
        spec = plumpy.ValueSpec("required_value", valid_type=int)

        self.assertIsNone(spec.validate(5))
        self.assertIsNotNone(spec.validate('a'))

    def test_validator(self):

        def validate(value):
            if not isinstance(value, int):
                return "Not int"
            return None

        spec = plumpy.ValueSpec("valid_with_validator", validator=validate)

        self.assertIsNone(spec.validate(5))
        self.assertIsNotNone(spec.validate('s'))
