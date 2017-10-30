from .util import TestCase

from plum.port import InputPort
from plum.exceptions import ValidationError


class TestInputPort(TestCase):
    def test_default(self):
        ip = InputPort('test', default=5)
        self.assertEqual(ip.default, 5)

        with self.assertRaises(ValidationError):
            InputPort('test', default=4, valid_type=str)

    def test_serialize(self):
        ip = InputPort('test', valid_type=int, serialize_fct=int)
        self.assertEqual(ip.evaluate('5'), 5)
        self.assertEqual(ip.evaluate(3), 3)
