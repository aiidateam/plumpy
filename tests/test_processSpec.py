import unittest
from plum.process import ProcessSpec


class StrSubtype(str):
    pass


class TestProcessSpec(unittest.TestCase):
    def setUp(self):
        self.spec = ProcessSpec()

    def test_dynamic_output(self):
        self.spec.dynamic_output(valid_type=str)
        port = self.spec.get_dynamic_output()
        self.assertTrue(port.validate("foo")[0])
        self.assertTrue(port.validate(StrSubtype("bar"))[0])
        self.assertFalse(port.validate(5)[0])

