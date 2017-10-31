from .util import TestCase

from plum.port import InputPort


class TestProcessSpec(TestCase):
    def test_default(self):
        ip = InputPort('test', default=5)
        self.assertEqual(ip.default, 5)

        with self.assertRaises(ValueError):
            InputPort('test', default=4, valid_type=str)