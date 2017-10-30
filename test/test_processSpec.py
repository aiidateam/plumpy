import unittest
from plum.process import ProcessSpec
from plum.exceptions import ValidationError
from .util import TestCase



class StrSubtype(str):
    pass


class TestProcessSpec(TestCase):
    def setUp(self):
        self.spec = ProcessSpec()

    def test_dynamic_output(self):
        self.spec.dynamic_output(valid_type=str)
        port = self.spec.get_dynamic_output()
        port.validate("foo")
        port.validate(StrSubtype("bar"))
        self.assertRaises(ValidationError, port.validate, 5)

        # Remove dynamic output
        self.spec.no_dynamic_output()
        self.assertIsNone(self.spec.get_dynamic_output())

        # Should be able to remove again
        self.spec.no_dynamic_output()
        self.assertIsNone(self.spec.get_dynamic_output())

        # Now add and check behaviour
        self.spec.dynamic_output(valid_type=str)
        port = self.spec.get_dynamic_output()
        port.validate("foo")
        port.validate(StrSubtype("bar"))
        self.assertRaises(ValidationError, port.validate, 5)

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

    def test_evaluate(self):
        """
        Test the global spec validator functionality.
        """
        def is_valid(spec, inputs):
            if ('a' in inputs) ^ ('b' in inputs):
                return True, None
            else:
                return False, "Must have a OR b in inputs"

        self.spec.input("a", required=False)
        self.spec.input("b", required=False)
        self.spec.validator(is_valid)

        self.assertRaises(ValidationError, self.spec.evaluate, inputs={})
        self.assertRaises(ValidationError, self.spec.evaluate, inputs={'a': 'a', 'b': 'b'})
        self.spec.evaluate(inputs={'a': 'a'})
        self.spec.evaluate(inputs={'b': 'b'})

    def test_serialize(self):
        self.spec.input("a", valid_type=int, serialize_fct=int)
        self.assertEqual(self.spec.evaluate(inputs={'a': '1'}), {'a': 1})
