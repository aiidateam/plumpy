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

        # Remove dynamic output
        self.spec.no_dynamic_output()
        self.assertIsNone(self.spec.get_dynamic_output())

        # Should be able to remove again
        self.spec.no_dynamic_output()
        self.assertIsNone(self.spec.get_dynamic_output())

        # Now add and check behaviour
        self.spec.dynamic_output(valid_type=str)
        port = self.spec.get_dynamic_output()
        self.assertTrue(port.validate("foo")[0])
        self.assertTrue(port.validate(StrSubtype("bar"))[0])
        self.assertFalse(port.validate(5)[0])

    def test_get_description(self):
        spec = ProcessSpec()
        # Initially there is no description
        self.assertEquals(spec.get_description(), "")

        # Adding an input should create some description
        spec.input("test")
        desc = spec.get_description()
        self.assertNotEqual(desc, "")

        # Similar with adding output
        spec = ProcessSpec()
        spec.output("test")
        desc = spec.get_description()
        self.assertNotEqual(desc, "")


