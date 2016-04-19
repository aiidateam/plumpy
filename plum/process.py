# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
from plum.port import InputPort, OutputPort, DynamicOutputPort
import plum.execution_engine as execution_engine
import plum.util as util


class ProcessSpec(object):
    """
    A class that defines the specifications of a Process, this includes what
    its inputs, outputs, etc are.

    All methods to modify the spec should be statements that describe the spec
    e.g.: input, output

    Every Process class has one of these.
    """
    def __init__(self):
        self._inputs = {}
        self._outputs = {}
        self._sealed = False

    def seal(self):
        """
        Seal this specification disallowing any further changes.
        :return:
        """
        self._sealed = True

    @property
    def sealed(self):
        return self._sealed

    @property
    def inputs(self):
        return self._inputs

    @property
    def outputs(self):
        return self._outputs

    def get_input(self, name):
        return self._inputs[name]

    def get_output(self, name):
        return self._outputs[name]

    def has_output(self, name):
        return name in self._outputs

    def has_dynamic_output(self):
        return self.has_output(DynamicOutputPort.NAME)

    def input(self, name, **kwargs):
        self.input_port(name, InputPort(self, name, **kwargs))

    def input_port(self, name, port):
        if self.sealed:
            raise RuntimeError("Cannot add an input after spec is sealed")
        if not isinstance(port, InputPort):
            raise TypeError("Output port must be an instance of InputPort")
        if name in self._inputs:
            raise ValueError("Input {} already exists.".format(name))

        self._inputs[name] = port

    def remove_input(self, name):
        if self.sealed:
            raise RuntimeError("Cannot remove an input after spec is sealed")
        self._inputs.pop(name)

    def output(self, name, **kwargs):
        self.output_port(name, OutputPort(self, name, **kwargs))

    def output_port(self, name, port):
        if self.sealed:
            raise RuntimeError("Cannot add an output after spec is sealed")
        if not isinstance(port, OutputPort):
            raise TypeError("Output port must be an instance of OutputPort")
        if name in self._outputs:
            raise ValueError("Output {} already exists.".format(name))

        self._outputs[name] = port

    def dynamic_output(self):
        self.output_port(DynamicOutputPort.NAME, DynamicOutputPort(self))

    def remove_dynamic_output(self):
        self.remove_output(DynamicOutputPort.NAME)

    def remove_output(self, name):
        if self.sealed:
            raise RuntimeError("Cannot remove an input after spec is sealed")
        self._outputs.pop(name)


class ProcessListener(object):
    __metaclass__ = ABCMeta

    def on_process_starting(self, process, inputs):
        pass

    def on_process_finalising(self, process):
        pass

    def on_process_finished(self, process, retval):
        pass

    def on_output_emitted(self, process, output_port, value, dynamic):
        pass


class Process(object):
    __metaclass__ = ABCMeta

    class RunScope(object):
        """
        A context manager to be used as:

        with RunScope(...):
          self._run()

        It defines the scope of a process execution and produces the internal
        event messages at the beginning and end of the scope as well as other
        internal process management.
        """
        def __init__(self, process, inputs, exec_engine):
            self._process = process
            self._inputs = inputs
            self._exec_engine = exec_engine

        def __enter__(self):
            self._process._on_process_starting(self._inputs)
            self._process._exec_engine = self._exec_engine

        def __exit__(self, type, value, traceback):
            self._process._exec_engine = None
            self._process._on_process_finalising()

    class ContinueScope(object):
        """
        A context manager to be used as:

        with ContinueScope(...):
          self._continue_from()

        It defines the scope of a process execution and produces the internal
        event messages at the end of the scope as well as other
        internal process management.
        """
        def __init__(self, process, exec_engine):
            self._process = process
            self._exec_engine = exec_engine

        def __enter__(self):
            self._process._exec_engine = self._exec_engine

        def __exit__(self, type, value, traceback):
            self._process._exec_engine = None
            self._process._on_process_finalising()

    # Static class stuff ######################
    _DEFAULT_EXEC_ENGINE = execution_engine.SerialEngine
    _spec_type = ProcessSpec

    @staticmethod
    def _define(spec):
        pass

    @classmethod
    def spec(cls):
        try:
            return cls._spec
        except AttributeError:
            cls._spec = cls._spec_type()
            cls._define(cls._spec)
            return cls._spec

    @classmethod
    def create(cls):
        return cls()

    @classmethod
    def get_name(cls):
        return cls.__name__

    @classmethod
    def _create_default_exec_engine(cls):
        """
        Crate the default execution engine.  Used if the one isn't supplied
        to the run method.
        :return: An instance of an ExceutionEngine.
        """
        return execution_engine.SerialEngine()
    ############################################

    def __init__(self):
        # Don't allow the spec to be changed anymore
        self.spec().seal()

        self._exec_engine = None
        self._output_values = {}
        self._proc_evt_helper = util.EventHelper(ProcessListener)

    def __call__(self, **kwargs):
        return self.run(kwargs)

    def add_process_listener(self, listener):
        assert (listener != self)
        self._proc_evt_helper.add_listener(listener)

    def remove_process_listener(self, listener):
        self._proc_evt_helper.remove_listener(listener)

    def run(self, inputs=None, exec_engine=None):
        if inputs is None:
            inputs = {}

        self._check_inputs(inputs)
        self._output_values = {}

        # Fill out the arguments
        ins = self._create_input_args(inputs)

        if not exec_engine:
            exec_engine = self._create_default_exec_engine()
        with self.RunScope(self, ins, exec_engine):
            retval = self._run(**ins)

        self._check_outputs()
        self._on_process_finished(retval)

        return retval

    def get_last_outputs(self):
        return self._output_values

    def _get_exec_engine(self):
        return self._exec_engine

    def _out(self, output_port, value):
        dynamic = False
        # Do checks on the outputs
        try:
            # Check types (if known)
            port = self.spec().get_output(output_port)
            if port.type is not None and not isinstance(value, port.type):
                raise TypeError(
                    "Process returned output {} of wrong type."
                    "Expected {}, got {}".
                        format(output_port, port.type, type(value)))
        except KeyError:
            # The port is unknown, do we support dynamic outputs?
            if self.spec().has_dynamic_output():
                dynamic = True
            else:
                raise TypeError(
                    "Process trying to output on unknown output port {}, "
                    "and does not have a dynamic output port in spec.".
                    format(output_port))

        self._output_values[output_port] = value
        self._on_output_emitted(output_port, value, dynamic)

    def _create_input_args(self, inputs):
        """
        Take the passed input arguments and fill in any default values for
        inputs that have no been supplied.

        Preconditions:
        * All required inputs have been supplied
        :param inputs: The supplied input values.
        :return: A dictionary of inputs including any with default values
        """
        kwargs = {}
        for name, port in self.spec().inputs.iteritems():
            if name in inputs:
                kwargs[name] = inputs[name]
            elif port.default:
                kwargs[name] = port.default
            else:
                assert (not port.required)

        return kwargs

    def _check_inputs(self, inputs):
        # Check all the required inputs are specified
        for name, port in self.spec().inputs.iteritems():
            if name not in inputs and port.default is None:
                raise RuntimeError(
                    "Cannot run process {} because port {}"
                    " is not filled".format(self.get_name(), name))

    def _check_outputs(self):
        # Check that the necessary outputs have been emitted
        for name, port in self.spec().outputs.iteritems():
            if port.required and name not in self._output_values:
                raise RuntimeError("A required output port ({}) was not "
                                   "produced by the process".format(name))

    @abstractmethod
    def _run(self, **kwargs):
        pass

    # Process messages ##################################################
    # Make sure to call the superclass if your override any of these ####
    def _on_process_starting(self, inputs):
        """
        Called when the inputs of a process passed checks and the process
        is about to begin.

        Any class overriding this method should make sure to call the super
        method, usually at the end of the function.

        :param inputs: The inputs the process is starting with
        """
        self._proc_evt_helper.fire_event('on_process_starting',
                                         self, inputs)

    def _on_process_finalising(self):
        """
        Called when the process has completed execution, however this may be
        the result of returning or an exception being raised.  Either way this
        message is guaranteed to be sent.  Only upon successful return and
        outputs passing checks would _on_process_finished be called.
        """
        self._proc_evt_helper.fire_event('on_process_finalising', self)

    def _on_process_finished(self, retval):
        """
        Called when the process has finished and the outputs have passed
        checks
        :param retval: The return value from the process
        """
        self._proc_evt_helper.fire_event('on_process_finished',
                                         self, retval)

    def _on_output_emitted(self, output_port, value, dynamic):
        self._proc_evt_helper.fire_event('on_output_emitted',
                                         self, output_port, value, dynamic)
    #####################################################################


class FunctionProcess(Process):
    # These will be replaced by build
    _output_name = None
    _func_args = None
    _func = None

    @staticmethod
    def build(func, output_name="value"):
        import inspect

        args, varargs, keywords, defaults = inspect.getargspec(func)

        def _define(spec):
            for i in range(len(args)):
                default = None
                if defaults and len(defaults) - len(args) + i >= 0:
                    default = defaults[i]
                spec.input(args[i], default=default)

            spec.output(output_name)

        return type(func.__name__, (FunctionProcess,),
                    {'_func': func,
                     '_define': staticmethod(_define),
                     '_func_args': args,
                     '_output_name': output_name})

    def __init__(self):
        super(FunctionProcess, self).__init__()

    def _run(self, **kwargs):
        args = []
        for arg in self._func_args:
            args.append(kwargs.pop(arg))

        self._out(self._output_name, self._func(*args))


