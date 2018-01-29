from . import utils
from plum.port import InputPort, PortNamespace
from plum.process import Process
from plum.test_utils import NewLoopProcess

class TestExposeProcess(utils.TestCaseWithLoop):

    def setUp(self):
        super(TestExposeProcess, self).setUp()

        class BaseProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(BaseProcess, cls).define(spec)
                spec.input('a', valid_type=str, default='a')
                spec.input('b', valid_type=str, default='b')
                spec.dynamic_input(valid_type=str)

        class ExposeProcess(NewLoopProcess):
            @classmethod
            def define(cls, spec):
                super(ExposeProcess, cls).define(spec)
                spec.expose_inputs(BaseProcess, namespace='base.name.space')
                spec.input('c', valid_type=int, default=1)
                spec.input('d', valid_type=int, default=2)
                spec.dynamic_input(valid_type=int)

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
        self.assertEquals(len(inputs), 3)
        self.assertTrue('c' in inputs)
        self.assertTrue('d' in inputs)
        self.assertEquals(inputs['c'].default, 1)
        self.assertEquals(inputs['d'].default, 2)

    def test_expose_ports(self):
        """
        Test that the exposed ports are present and properly deepcopied
        """
        exposed_inputs = self.ExposeProcess.spec().inputs.get_port('base.name.space')

        self.assertEquals(len(exposed_inputs), 2)
        self.assertTrue('a' in exposed_inputs)
        self.assertTrue('b' in exposed_inputs)
        self.assertEquals(exposed_inputs['a'].default, 'a')
        self.assertEquals(exposed_inputs['b'].default, 'b')

        # Change the default of base process port and verify they don't change the exposed port
        self.BaseProcess.spec().inputs['a'].set_default('c')
        self.assertEquals(self.BaseProcess.spec().inputs['a'].default, 'c')
        self.assertEquals(exposed_inputs['a'].default, 'a')

    def test_expose_attributes(self):
        """
        Test that the attributes of the exposed PortNamespace are maintained and properly deepcopied
        """
        inputs = self.ExposeProcess.spec().inputs
        exposed_inputs = self.ExposeProcess.spec().inputs.get_port('base.name.space')

        self.assertEquals(self.BaseProcess.spec().inputs.valid_type, str)
        self.assertEquals(exposed_inputs.valid_type, str)
        self.assertEquals(inputs.valid_type, int)

        # Now change the valid type of the BaseProcess inputs and verify it does not affect ExposeProcess
        self.BaseProcess.spec().inputs.set_valid_type(float)

        self.assertEquals(self.BaseProcess.spec().inputs.valid_type, float)
        self.assertEquals(exposed_inputs.valid_type, str)
        self.assertEquals(inputs.valid_type, int)

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

        self.assertEquals(len(inputs), 3)
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

        self.assertEquals(len(inputs), 3)
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