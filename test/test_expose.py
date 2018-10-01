from __future__ import absolute_import
from . import utils
from plumpy.ports import PortNamespace
from plumpy.process_spec import ProcessSpec
from plumpy.test_utils import NewLoopProcess


class TestExposeProcess(utils.TestCaseWithLoop):

    def setUp(self):
        super(TestExposeProcess, self).setUp()

        class BaseProcess(NewLoopProcess):

            @classmethod
            def define(cls, spec):
                super(BaseProcess, cls).define(spec)
                spec.input('a', valid_type=str, default='a')
                spec.input('b', valid_type=str, default='b')
                spec.inputs.dynamic = True
                spec.inputs.valid_type = str

        class ExposeProcess(NewLoopProcess):

            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(BaseProcess, namespace='base.name.space')
                spec.input('c', valid_type=int, default=1)
                spec.input('d', valid_type=int, default=2)
                spec.inputs.dynamic = True
                spec.inputs.valid_type = int

        self.BaseProcess = BaseProcess
        self.ExposeProcess = ExposeProcess

    def test_expose_nested_namespace(self):
        """
        Test that expose_inputs can create nested namespaces while maintaining own ports
        """
        inputs = self.ExposeProcess.spec().inputs

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
        """
        Test that the exposed ports are present and properly deepcopied
        """
        exposed_inputs = self.ExposeProcess.spec().inputs.get_port('base.name.space')

        self.assertEqual(len(exposed_inputs), 2)
        self.assertTrue('a' in exposed_inputs)
        self.assertTrue('b' in exposed_inputs)
        self.assertEqual(exposed_inputs['a'].default, 'a')
        self.assertEqual(exposed_inputs['b'].default, 'b')

        # Change the default of base process port and verify they don't change the exposed port
        self.BaseProcess.spec().inputs['a'].default = 'c'
        self.assertEqual(self.BaseProcess.spec().inputs['a'].default, 'c')
        self.assertEqual(exposed_inputs['a'].default, 'a')

    def test_expose_attributes(self):
        """
        Test that the attributes of the exposed PortNamespace are maintained and properly deepcopied
        """
        inputs = self.ExposeProcess.spec().inputs
        exposed_inputs = self.ExposeProcess.spec().inputs.get_port('base.name.space')

        self.assertEqual(self.BaseProcess.spec().inputs.valid_type, str)
        self.assertEqual(exposed_inputs.valid_type, str)
        self.assertEqual(inputs.valid_type, int)

        # Now change the valid type of the BaseProcess inputs and verify it does not affect ExposeProcess
        self.BaseProcess.spec().inputs.valid_type = float

        self.assertEqual(self.BaseProcess.spec().inputs.valid_type, float)
        self.assertEqual(exposed_inputs.valid_type, str)
        self.assertEqual(inputs.valid_type, int)

    def test_expose_exclude(self):
        """
        Test that the exclude argument of exposed_inputs works correctly and excludes ports from being absorbed
        """
        BaseProcess = self.BaseProcess

        class ExcludeProcess(NewLoopProcess):

            @classmethod
            def define(cls, spec):
                super(ExcludeProcess, cls).define(spec)
                spec.expose_inputs(BaseProcess, exclude=('a',))
                spec.input('c', valid_type=int, default=1)
                spec.input('d', valid_type=int, default=2)

        inputs = ExcludeProcess.spec().inputs

        self.assertEqual(len(inputs), 3)
        self.assertTrue('a' not in inputs)

    def test_expose_include(self):
        """
        Test that the include argument of exposed_inputs works correctly and includes only specified ports
        """
        BaseProcess = self.BaseProcess

        class ExcludeProcess(NewLoopProcess):

            @classmethod
            def define(cls, spec):
                super(ExcludeProcess, cls).define(spec)
                spec.expose_inputs(BaseProcess, include=('b',))
                spec.input('c', valid_type=int, default=1)
                spec.input('d', valid_type=int, default=2)

        inputs = ExcludeProcess.spec().inputs

        self.assertEqual(len(inputs), 3)
        self.assertTrue('a' not in inputs)

    def test_expose_exclude_include_mutually_exclusive(self):
        """
        Test that passing both exclude and include raises
        """
        BaseProcess = self.BaseProcess

        class ExcludeProcess(NewLoopProcess):

            @classmethod
            def define(cls, spec):
                super(ExcludeProcess, cls).define(spec)
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

        def validator_function(input):
            pass

        # Define child process with all mutable properties of the inputs PortNamespace to a non-default value
        # This way we can check if the defaults of the ParentProcessSpec will be properly overridden
        ChildProcessSpec = ProcessSpec()
        ChildProcessSpec.input('a', valid_type=int)
        ChildProcessSpec.input('b', valid_type=str)
        ChildProcessSpec.inputs.validator = validator_function
        ChildProcessSpec.inputs.valid_type = bool
        ChildProcessSpec.inputs.required = False
        ChildProcessSpec.inputs.dynamic = True
        ChildProcessSpec.inputs.default = True
        ChildProcessSpec.inputs.help = 'testing'

        ParentProcessSpec = ProcessSpec()
        ParentProcessSpec.input('c', valid_type=float)
        ParentProcessSpec._expose_ports(
            process_class=None,
            source=ChildProcessSpec.inputs,
            destination=ParentProcessSpec.inputs,
            expose_memory=ParentProcessSpec._exposed_inputs,
            namespace=None,
            exclude=(),
            include=None,
            namespace_options={})

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

        def validator_function(input):
            pass

        # Define child process with all mutable properties of the inputs PortNamespace to a non-default value
        # This way we can check if the defaults of the ParentProcessSpec will be properly overridden
        ChildProcessSpec = ProcessSpec()
        ChildProcessSpec.input('a', valid_type=int)
        ChildProcessSpec.input('b', valid_type=str)
        ChildProcessSpec.inputs.validator = validator_function
        ChildProcessSpec.inputs.valid_type = bool
        ChildProcessSpec.inputs.required = False
        ChildProcessSpec.inputs.dynamic = True
        ChildProcessSpec.inputs.default = True
        ChildProcessSpec.inputs.help = 'testing'

        ParentProcessSpec = ProcessSpec()
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
            })

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

        def validator_function(input):
            pass

        # Define child process with all mutable properties of the inputs PortNamespace to a non-default value
        # This way we can check if the defaults of the ParentProcessSpec will be properly overridden
        ChildProcessSpec = ProcessSpec()
        ChildProcessSpec.input('a', valid_type=int)
        ChildProcessSpec.input('b', valid_type=str)
        ChildProcessSpec.inputs.validator = validator_function
        ChildProcessSpec.inputs.valid_type = bool
        ChildProcessSpec.inputs.required = False
        ChildProcessSpec.inputs.dynamic = True
        ChildProcessSpec.inputs.default = True
        ChildProcessSpec.inputs.help = 'testing'

        ParentProcessSpec = ProcessSpec()
        ParentProcessSpec.input('c', valid_type=float)
        ParentProcessSpec._expose_ports(
            process_class=None,
            source=ChildProcessSpec.inputs,
            destination=ParentProcessSpec.inputs,
            expose_memory=ParentProcessSpec._exposed_inputs,
            namespace='namespace',
            exclude=(),
            include=None,
            namespace_options={})

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
        ChildProcessSpec = ProcessSpec()
        ParentProcessSpec = ProcessSpec()

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
                })
