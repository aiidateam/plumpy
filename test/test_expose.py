# -*- coding: utf-8 -*-
import unittest

from plumpy.ports import PortNamespace
from plumpy.process_spec import ProcessSpec
from plumpy.processes import Process

from test.utils import NewLoopProcess


def validator_function(input, port):
    pass


class BaseNamespaceProcess(NewLoopProcess):
    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input('top')
        spec.input('namespace.sub_one')
        spec.input('namespace.sub_two')
        spec.inputs['namespace'].valid_type = (int, float)
        spec.inputs['namespace'].validator = validator_function


class BaseProcess(NewLoopProcess):
    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input('a', valid_type=str, default='a')
        spec.input('b', valid_type=str, default='b')
        spec.inputs.dynamic = True
        spec.inputs.valid_type = str


class ExposeProcess(NewLoopProcess):
    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.expose_inputs(BaseProcess, namespace='base.name.space')
        spec.input('c', valid_type=int, default=1)
        spec.input('d', valid_type=int, default=2)
        spec.inputs.dynamic = True
        spec.inputs.valid_type = int


class TestExposeProcess(unittest.TestCase):
    def check_ports(self, process, namespace, expected_port_names):
        """Check the port namespace of a given process inputs spec for existence of set of expected port names."""
        port_namespace = process.spec().inputs

        if namespace is not None:
            port_namespace = process.spec().inputs.get_port(namespace)

        self.assertEqual(set(port_namespace.keys()), set(expected_port_names))

    def check_namespace_properties(self, process_left, namespace_left, process_right, namespace_right):
        """Check that all properties, with exception of ports, of two port namespaces are equal."""
        if not issubclass(process_left, Process) or not issubclass(process_right, Process):
            raise TypeError('`process_left` and `process_right` should be processes')

        port_namespace_left = process_left.spec().inputs.get_port(namespace_left)
        port_namespace_right = process_right.spec().inputs.get_port(namespace_right)

        left_dict = {k: v for k, v in port_namespace_left.__dict__.items() if k != '_ports'}
        right_dict = {k: v for k, v in port_namespace_right.__dict__.items() if k != '_ports'}

        self.assertEqual(left_dict, right_dict)

    def test_expose_dynamic(self):
        """Test that exposing a dynamic namespace remains dynamic."""

        class Lower(Process):
            @classmethod
            def define(cls, spec):
                super(Lower, cls).define(spec)
                spec.input_namespace('foo', dynamic=True)

        class Upper(Process):
            @classmethod
            def define(cls, spec):
                super(Upper, cls).define(spec)
                spec.expose_inputs(Lower)

        self.assertTrue(Lower.spec().inputs['foo'].dynamic)
        self.assertTrue(Upper.spec().inputs['foo'].dynamic)

    def test_expose_nested_namespace(self):
        """Test that expose_inputs can create nested namespaces while maintaining own ports."""
        inputs = ExposeProcess.spec().inputs

        # Verify that the nested namespaces are present
        self.assertTrue('base' in inputs)
        self.assertTrue('name' in inputs['base'])
        self.assertTrue('space' in inputs['base']['name'])

        exposed_inputs = inputs.get_port('base.name.space')

        self.assertTrue(isinstance(exposed_inputs, PortNamespace))

        # Verify that own ports are left untouched (should be three ports, 'c', 'd' and 'base')
        self.assertEqual(len(inputs), 3)
        self.assertTrue('c' in inputs)
        self.assertTrue('d' in inputs)
        self.assertEqual(inputs['c'].default, 1)
        self.assertEqual(inputs['d'].default, 2)

    def test_expose_ports(self):
        """Test that the exposed ports are present and properly deepcopied."""
        exposed_inputs = ExposeProcess.spec().inputs.get_port('base.name.space')

        self.assertEqual(len(exposed_inputs), 2)
        self.assertTrue('a' in exposed_inputs)
        self.assertTrue('b' in exposed_inputs)
        self.assertEqual(exposed_inputs['a'].default, 'a')
        self.assertEqual(exposed_inputs['b'].default, 'b')

        # Change the default of base process port and verify they don't change the exposed port
        BaseProcess.spec().inputs['a'].default = 'c'
        self.assertEqual(BaseProcess.spec().inputs['a'].default, 'c')
        self.assertEqual(exposed_inputs['a'].default, 'a')

    def test_expose_attributes(self):
        """Test that the attributes of the exposed PortNamespace are maintained and properly deepcopied."""
        inputs = ExposeProcess.spec().inputs
        exposed_inputs = ExposeProcess.spec().inputs.get_port('base.name.space')

        self.assertEqual(str, BaseProcess.spec().inputs.valid_type)
        self.assertEqual(str, exposed_inputs.valid_type)
        self.assertEqual(int, inputs.valid_type)

        # Now change the valid type of the BaseProcess inputs and verify it does not affect ExposeProcess
        BaseProcess.spec().inputs.valid_type = float

        self.assertEqual(BaseProcess.spec().inputs.valid_type, float)
        self.assertEqual(exposed_inputs.valid_type, str)
        self.assertEqual(inputs.valid_type, int)

    def test_expose_exclude(self):
        """Test that the exclude argument of exposed_inputs works correctly and excludes ports from being absorbed."""

        class ExcludeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.expose_inputs(BaseProcess, exclude=('a',))
                spec.input('c', valid_type=int, default=1)
                spec.input('d', valid_type=int, default=2)

        inputs = ExcludeProcess.spec().inputs

        self.assertEqual(len(inputs), 3)
        self.assertTrue('a' not in inputs)

    def test_expose_include(self):
        """Test that the include argument of exposed_inputs works correctly and includes only specified ports."""

        class ExcludeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.expose_inputs(BaseProcess, include=('b',))
                spec.input('c', valid_type=int, default=1)
                spec.input('d', valid_type=int, default=2)

        inputs = ExcludeProcess.spec().inputs

        self.assertEqual(len(inputs), 3)
        self.assertTrue('a' not in inputs)

    def test_expose_exclude_include_mutually_exclusive(self):
        """Test that passing both exclude and include raises."""

        class ExcludeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.expose_inputs(BaseProcess, exclude=('a',), include=('b',))
                spec.input('c', valid_type=int, default=1)
                spec.input('d', valid_type=int, default=2)

        with self.assertRaises(ValueError):
            ExcludeProcess.spec()

    def test_expose_ports_top_level(self):
        """
        Verify that exposing a sub process in top level correctly overrides the parent's namespace
        properties with that of the exposed process
        """

        def validator_function(input, port):
            pass

        # Define child process with all mutable properties of the inputs PortNamespace to a non-default value
        # This way we can check if the defaults of the ParentProcessSpec will be properly overridden
        ChildProcessSpec = ProcessSpec()  # noqa: N806
        ChildProcessSpec.input('a', valid_type=int)
        ChildProcessSpec.input('b', valid_type=str)
        ChildProcessSpec.inputs.validator = validator_function
        ChildProcessSpec.inputs.valid_type = bool
        ChildProcessSpec.inputs.required = False
        ChildProcessSpec.inputs.dynamic = True
        ChildProcessSpec.inputs.default = True
        ChildProcessSpec.inputs.help = 'testing'

        ParentProcessSpec = ProcessSpec()  # noqa: N806
        ParentProcessSpec.input('c', valid_type=float)
        ParentProcessSpec._expose_ports(
            process_class=None,
            source=ChildProcessSpec.inputs,
            destination=ParentProcessSpec.inputs,
            expose_memory=ParentProcessSpec._exposed_inputs,
            namespace=None,
            exclude=(),
            include=None,
            namespace_options={},
        )

        # Verify that all the ports are there
        self.assertIn('a', ParentProcessSpec.inputs)
        self.assertIn('b', ParentProcessSpec.inputs)
        self.assertIn('c', ParentProcessSpec.inputs)

        # Verify that all the port namespace attributes are copied over
        self.assertEqual(ParentProcessSpec.inputs.validator, validator_function)
        self.assertEqual(ParentProcessSpec.inputs.valid_type, bool)
        self.assertEqual(ParentProcessSpec.inputs.required, False)
        self.assertEqual(ParentProcessSpec.inputs.dynamic, True)
        self.assertEqual(ParentProcessSpec.inputs.default, True)
        self.assertEqual(ParentProcessSpec.inputs.help, 'testing')

    def test_expose_ports_top_level_override(self):
        """
        Verify that exposing a sub process in top level correctly overrides the parent's namespace
        properties with that of the exposed process, but that any valid property passed in the
        namespace_options will be the end-all-be-all
        """

        def validator_function(input, port):
            pass

        # Define child process with all mutable properties of the inputs PortNamespace to a non-default value
        # This way we can check if the defaults of the ParentProcessSpec will be properly overridden
        ChildProcessSpec = ProcessSpec()  # noqa: N806
        ChildProcessSpec.input('a', valid_type=int)
        ChildProcessSpec.input('b', valid_type=str)
        ChildProcessSpec.inputs.validator = validator_function
        ChildProcessSpec.inputs.valid_type = bool
        ChildProcessSpec.inputs.required = False
        ChildProcessSpec.inputs.dynamic = True
        ChildProcessSpec.inputs.default = True
        ChildProcessSpec.inputs.help = 'testing'

        ParentProcessSpec = ProcessSpec()  # noqa: N806
        ParentProcessSpec.input('c', valid_type=float)
        ParentProcessSpec._expose_ports(
            process_class=None,
            source=ChildProcessSpec.inputs,
            destination=ParentProcessSpec.inputs,
            expose_memory=ParentProcessSpec._exposed_inputs,
            namespace=None,
            exclude=(),
            include=None,
            namespace_options={
                'validator': None,
                'valid_type': None,
                'required': True,
                'dynamic': False,
                'default': None,
                'help': None,
            },
        )

        # Verify that all the ports are there
        self.assertIn('a', ParentProcessSpec.inputs)
        self.assertIn('b', ParentProcessSpec.inputs)
        self.assertIn('c', ParentProcessSpec.inputs)

        # Verify that all the port namespace attributes correspond to the values passed in the namespace_options
        self.assertEqual(ParentProcessSpec.inputs.validator, None)
        self.assertEqual(ParentProcessSpec.inputs.valid_type, None)
        self.assertEqual(ParentProcessSpec.inputs.required, True)
        self.assertEqual(ParentProcessSpec.inputs.dynamic, False)
        self.assertEqual(ParentProcessSpec.inputs.default, None)
        self.assertEqual(ParentProcessSpec.inputs.help, None)

    def test_expose_ports_namespace(self):
        """
        Verify that exposing a sub process in a namespace correctly overrides the defaults of the new
        namespace with the properties of the exposed port namespace
        """

        def validator_function(input, port):
            pass

        # Define child process with all mutable properties of the inputs PortNamespace to a non-default value
        # This way we can check if the defaults of the ParentProcessSpec will be properly overridden
        ChildProcessSpec = ProcessSpec()  # noqa: N806
        ChildProcessSpec.input('a', valid_type=int)
        ChildProcessSpec.input('b', valid_type=str)
        ChildProcessSpec.inputs.validator = validator_function
        ChildProcessSpec.inputs.valid_type = bool
        ChildProcessSpec.inputs.required = False
        ChildProcessSpec.inputs.dynamic = True
        ChildProcessSpec.inputs.default = True
        ChildProcessSpec.inputs.help = 'testing'

        ParentProcessSpec = ProcessSpec()  # noqa: N806
        ParentProcessSpec.input('c', valid_type=float)
        ParentProcessSpec._expose_ports(
            process_class=None,
            source=ChildProcessSpec.inputs,
            destination=ParentProcessSpec.inputs,
            expose_memory=ParentProcessSpec._exposed_inputs,
            namespace='namespace',
            exclude=(),
            include=None,
            namespace_options={},
        )

        # Verify that all the ports are there
        self.assertIn('a', ParentProcessSpec.inputs['namespace'])
        self.assertIn('b', ParentProcessSpec.inputs['namespace'])
        self.assertIn('c', ParentProcessSpec.inputs)

        # Verify that all the port namespace attributes are copied over
        self.assertEqual(ParentProcessSpec.inputs['namespace'].validator, validator_function)
        self.assertEqual(ParentProcessSpec.inputs['namespace'].valid_type, bool)
        self.assertEqual(ParentProcessSpec.inputs['namespace'].required, False)
        self.assertEqual(ParentProcessSpec.inputs['namespace'].dynamic, True)
        self.assertEqual(ParentProcessSpec.inputs['namespace'].default, True)
        self.assertEqual(ParentProcessSpec.inputs['namespace'].help, 'testing')

    def test_expose_ports_namespace_options_non_existent(self):
        """
        Verify that passing non-supported PortNamespace mutable properties in namespace_options
        will raise a ValueError
        """
        ChildProcessSpec = ProcessSpec()  # noqa: N806
        ParentProcessSpec = ProcessSpec()  # noqa: N806

        with self.assertRaises(ValueError):
            ParentProcessSpec._expose_ports(
                process_class=None,
                source=ChildProcessSpec.inputs,
                destination=ParentProcessSpec.inputs,
                expose_memory=ParentProcessSpec._exposed_inputs,
                namespace=None,
                exclude=(),
                include=None,
                namespace_options={
                    'non_existent': None,
                },
            )

    def test_expose_nested_include_top_level(self):
        """Test the include rules can be nested and are properly unwrapped."""

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.expose_inputs(BaseNamespaceProcess, namespace='base', include=('top',))

        self.check_ports(ExposeProcess, None, ['base'])
        self.check_ports(ExposeProcess, 'base', ['top'])

    def test_expose_nested_include_namespace(self):
        """Test the include rules can be nested and are properly unwrapped."""

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.expose_inputs(BaseNamespaceProcess, namespace='base', include=('namespace',))

        self.check_ports(ExposeProcess, None, ['base'])
        self.check_ports(ExposeProcess, 'base', ['namespace'])
        self.check_ports(ExposeProcess, 'base.namespace', ['sub_one', 'sub_two'])
        self.check_namespace_properties(BaseNamespaceProcess, 'namespace', ExposeProcess, 'base.namespace')

    def test_expose_nested_include_namespace_sub(self):
        """Test the include rules can be nested and are properly unwrapped."""

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.expose_inputs(BaseNamespaceProcess, namespace='base', include=('namespace.sub_two',))

        self.check_ports(ExposeProcess, None, ['base'])
        self.check_ports(ExposeProcess, 'base', ['namespace'])
        self.check_ports(ExposeProcess, 'base.namespace', ['sub_two'])
        self.check_namespace_properties(BaseNamespaceProcess, 'namespace', ExposeProcess, 'base.namespace')

    def test_expose_nested_include_combination(self):
        """Test the include rules can be nested and are properly unwrapped."""

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.expose_inputs(BaseNamespaceProcess, namespace='base', include=('namespace.sub_two', 'top'))

        self.check_ports(ExposeProcess, None, ['base'])
        self.check_ports(ExposeProcess, 'base', ['namespace', 'top'])
        self.check_ports(ExposeProcess, 'base.namespace', ['sub_two'])
        self.check_namespace_properties(BaseNamespaceProcess, 'namespace', ExposeProcess, 'base.namespace')

    def test_expose_nested_exclude_top_level(self):
        """Test the exclude rules can be nested and are properly unwrapped."""

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.expose_inputs(BaseNamespaceProcess, namespace='base', exclude=('top',))

        self.check_ports(ExposeProcess, None, ['base'])
        self.check_ports(ExposeProcess, 'base', ['namespace'])
        self.check_ports(ExposeProcess, 'base.namespace', ['sub_one', 'sub_two'])
        self.check_namespace_properties(BaseNamespaceProcess, 'namespace', ExposeProcess, 'base.namespace')

    def test_expose_nested_exclude_namespace(self):
        """Test the exclude rules can be nested and are properly unwrapped."""

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.expose_inputs(BaseNamespaceProcess, namespace='base', exclude=('namespace',))

        self.check_ports(ExposeProcess, None, ['base'])
        self.check_ports(ExposeProcess, 'base', ['top'])

    def test_expose_nested_exclude_namespace_sub(self):
        """Test the exclude rules can be nested and are properly unwrapped."""

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.expose_inputs(BaseNamespaceProcess, namespace='base', exclude=('namespace.sub_two',))

        self.check_ports(ExposeProcess, None, ['base'])
        self.check_ports(ExposeProcess, 'base', ['top', 'namespace'])
        self.check_ports(ExposeProcess, 'base.namespace', ['sub_one'])
        self.check_namespace_properties(BaseNamespaceProcess, 'namespace', ExposeProcess, 'base.namespace')

    def test_expose_nested_exclude_combination(self):
        """Test the exclude rules can be nested and are properly unwrapped."""

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.expose_inputs(BaseNamespaceProcess, namespace='base', exclude=('namespace.sub_two', 'top'))

        self.check_ports(ExposeProcess, None, ['base'])
        self.check_ports(ExposeProcess, 'base', ['namespace'])
        self.check_ports(ExposeProcess, 'base.namespace', ['sub_one'])
        self.check_namespace_properties(BaseNamespaceProcess, 'namespace', ExposeProcess, 'base.namespace')

    def test_expose_exclude_port_with_validator(self):
        """Test that validators of excluded ports are not called, even if the parent namespace is dynamic.

        This is a regression test for https://github.com/aiidateam/plumpy/issues/267. Changes to the method
        ``PortNamespace.get_port`` would recursively create and return non-existing ports as long as the parent
        namespace is dynamic. This would result in a problem with the validationn of namespaces that contained exposed
        namespaces with validators that are dependent on excluded ports. Even though the port was excluded, the changes
        in ``get_port`` would now recreate the port on the fly when the validation attempted to retrieve it, thereby
        undoing the exclusion of the port when exposed.
        """

        class BaseProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.input('a', required=False)
                spec.inputs.dynamic = True
                spec.inputs.validator = cls.validator

            @classmethod
            def validator(cls, value, ctx):
                try:
                    ctx.get_port('a')
                except ValueError:
                    return None

                if not isinstance(value['a'], str):
                    return f'value for input `a` should be a str, but got: {type(value['a'])}'

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super().define(spec)
                spec.expose_inputs(BaseProcess, namespace='base', exclude=('a',))

        assert ExposeProcess.spec().inputs.validate({}) is None
