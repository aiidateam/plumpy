from unittest import TestCase

from plum.port import InputPort


class TestProcessSpec(TestCase):
    def test_default(self):
        ip = InputPort(None, 'test', default=5)
        self.assertEqual(ip.default, 5)

        with self.assertRaises(ValueError):
            InputPort(None, 'test', default=4, valid_type=str)