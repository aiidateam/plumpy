# -*- coding: utf-8 -*-

from collections import namedtuple
from abc import ABCMeta, abstractmethod
from plum.port import InputPort, InputGroupPort, OutputPort,\
    Attribute, DynamicOutputPort, DynamicInputPort
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
        self._attributes = {}
        self._sealed = False

    def seal(self):
        """
        Seal this specification disallowing any further changes.
        """
        self._sealed = True

    @property
    def sealed(self):
        return self._sealed

    # Inputs ##################################################################
    @property
    def inputs(self):
        return self._inputs

    def get_input(self, name):
        return self._inputs[name]

    def has_input(self, name):
        return name in self._inputs

    def input(self, name, **kwargs):
        """
        Define an Process input.

        :param name: The name of the input.
        :param kwargs: The input port options.
        """
        self.input_port(name, InputPort(self, name, **kwargs))

    def dynamic_input(self):
        self.input_port(DynamicInputPort.NAME, DynamicInputPort(self))

    def remove_dynamic_input(self):
        self.remove_input(DynamicInputPort.NAME)

    def has_dynamic_input(self):
        return self.has_input(DynamicInputPort.NAME)

    def input_group(self, name, **kwargs):
        self.input_port(name, InputGroupPort(self, name, **kwargs))

    def input_port(self, name, port):
        if self.sealed:
            raise RuntimeError("Cannot add an input after spec is sealed")
        if not isinstance(port, InputPort):
            raise TypeError("Input port must be an instance of InputPort")
        if name in self._inputs:
            raise ValueError("Input {} already exists.".format(name))

        self._inputs[name] = port

    def remove_input(self, name):
        if self.sealed:
            raise RuntimeError("Cannot remove an input after spec is sealed")
        self._inputs.pop(name)
    ###########################################################################

    # Outputs #################################################################
    @property
    def outputs(self):
        return self._outputs

    def get_output(self, name):
        return self._outputs[name]

    def has_output(self, name):
        return name in self._outputs

    def has_dynamic_output(self):
        return self.has_output(DynamicOutputPort.NAME)

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
    ###########################################################################

    # Attributes ##############################################################
    @property
    def attributes(self):
        return self._attributes

    def get_attribute(self, name):
        return self._attributes[name]

    def has_attribute(self, name):
        return name in self._attributes

    def attribute(self, name, **kwargs):
        """
        Define an Process input.

        :param name: The name of the input.
        :param kwargs: The input port options.
        """
        self.attribute_value(name, Attribute(self, name, **kwargs))

    def attribute_value(self, name, attribute_spec):
        if self.sealed:
            raise RuntimeError("Cannot add an attribute after spec is sealed")
        if not isinstance(attribute_spec, Attribute):
            raise TypeError("Attribute specifications must be of type Attribute")
        if name in self._attributes:
            raise ValueError("Input {} already exists.".format(name))

        self._attributes[name] = attribute_spec

    def remove_attribute(self, name):
        if self.sealed:
            raise RuntimeError("Cannot remove an attribute after spec is sealed")
        self._attributes.pop(name)
    ###########################################################################


class ProcessListener(object):
    __metaclass__ = ABCMeta

    def on_process_starting(self, process, inputs):
        pass

    def on_process_waiting(self, process):
        pass

    def on_process_finalising(self, process):
        pass

    def on_process_finished(self, process, retval):
        pass

    def on_output_emitted(self, process, output_port, value, dynamic):
        pass


class Process(object):
    __metaclass__ = ABCMeta

    # Static class stuff ######################
    RunningData = namedtuple('RunningData',
                             ['exec_engine', 'inputs'])

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
    def create(cls, attributes=None):
        return cls(attributes)

    @classmethod
    def get_name(cls):
        return cls.__name__

    @classmethod
    def _create_default_exec_engine(cls):
        """
        Crate the default execution engine.  Used if the run() method is
        called instead of asking an execution engine to run this process.

        :return: An instance of ExceutionEngine.
        """
        from plum.serial_engine import SerialEngine
        return SerialEngine()
    ############################################

    def __init__(self, attributes=None):
        # Don't allow the spec to be changed anymore
        self.spec().seal()

        self._attributes = self._check_and_generate_attributes(attributes)
        self.__running_data = None
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
        if not exec_engine:
            exec_engine = self._create_default_exec_engine()
        return exec_engine.run(self, inputs)

    def get_last_outputs(self):
        return self._output_values

    def _get_exec_engine(self):
        return self.__running_data.exec_engine

    @property
    def _inputs(self):
        return self.__running_data.inputs

    def _out(self, output_port, value):
        dynamic = False
        # Do checks on the outputs
        try:
            # Check types (if known)
            port = self.spec().get_output(output_port)
            if port.valid_type is not None and not isinstance(value, port.valid_type):
                raise TypeError(
                    "Process returned output {} of wrong type."
                    "Expected {}, got {}".
                        format(output_port, port.valid_type, type(value)))
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

    # Inputs ##################################################################
    def _create_input_args(self, inputs):
        """
        Take the passed input arguments and fill in any default values for
        inputs that have no been supplied.

        Preconditions:
        * All required inputs have been supplied
        :param inputs: The supplied input values.
        :return: A dictionary of inputs including any with default values
        """
        ins = inputs.copy()
        # Go through the spec filling in any default and checking for required
        # inputs
        for name, port in self.spec().inputs.iteritems():
            if name not in ins:
                if port.default:
                    ins[name] = port.default
                elif port.required:
                    raise ValueError(
                        "Value not supplied for required inputs port {}".format(name))

        return ins

    def _check_inputs(self, inputs):
        # Check the inputs meet the requirements
        if not self.spec().has_dynamic_input():
            unexpected = set(inputs.iterkeys()) - set(self.spec().inputs.iterkeys())
            if unexpected:
                raise RuntimeError(
                    "Unexpected inputs found: {}.  If you want to allow dynamic "
                    "inputs add dynamic_input() to the spec definition.".
                    format(unexpected))

        for name, port in self.spec().inputs.iteritems():
            valid, msg = port.validate(inputs.get(name, None))
            if not valid:
                raise RuntimeError("Cannot run process {} because {}".format(self.get_name(), msg))
    ###########################################################################

    # Outputs #################################################################
    def _check_outputs(self):
        # Check that the necessary outputs have been emitted
        for name, port in self.spec().outputs.iteritems():
            valid, msg = port.validate(self._output_values.get(name, None))
            if not valid:
                raise RuntimeError("Process {} failed because {}".format(self.get_name(), msg))
    ###########################################################################

    # Attributes ##############################################################
    def _check_and_generate_attributes(self, attributes):
        """
        Take the passed attributes and fill in any default values for those
        that have no been supplied.

        Preconditions:
        * All required inputs have been supplied
        :param inputs: The supplied input values.
        :return: A dictionary of inputs including any with default values
        """
        attrs = {} if attributes is None else attributes.copy()

        # Check for any unknown attributes
        unexpected = set(attrs.iterkeys()) - set(self.spec().attributes.iterkeys())
        if unexpected:
            raise RuntimeError(
                "Unexpected inputs found: {}.  If you want to allow dynamic "
                "inputs add dynamic_input() to the spec definition.".
                format(unexpected))

        # Go through the spec filling in any default and checking for required
        # inputs
        for name, value_spec in self.spec().attributes.iteritems():
            if name in attrs:
                valid, msg = value_spec.validate(attrs[name])
                if not valid:
                    raise ValueError("Cannot run process {} because {}".format(self.get_name(), msg))
            if name not in attrs:
                if value_spec.default:
                    attrs[name] = value_spec.default
                elif value_spec.required:
                    raise ValueError(
                        "Value not supplied for required attribute {}".format(name))

        return attrs
    ###########################################################################

    @abstractmethod
    def _run(self, **kwargs):
        pass

    def save_instance_state(self, bundle):
        pass

    def load_instance_state(self, bundle):
        pass

    # Process messages ##################################################
    # These should only be called by an execution engine (or tests) #####
    # Make sure to call the superclass if your override any of these ####
    def on_start(self, inputs, exec_engine):
        """
        Called when the inputs of a process passed checks and the process
        is about to begin.

        Any class overriding this method should make sure to call the super
        method, usually at the end of the function.

        :param inputs: The inputs the process is starting with
        """
        self._check_inputs(inputs)
        self.__running_data = self.RunningData(
            inputs=util.AttributesFrozendict(inputs),
            exec_engine=exec_engine)
        self._proc_evt_helper.fire_event('on_process_starting',
                                         self, inputs)

    def on_wait(self):
        self._proc_evt_helper.fire_event('on_process_waiting', self)

    def on_finalise(self):
        """
        Called when the process has completed execution, however this may be
        the result of returning or an exception being raised.  Either way this
        message is guaranteed to be sent.  Only upon successful return and
        outputs passing checks would _on_process_finished be called.
        """
        self.__running_data = None
        self._check_outputs()
        self._proc_evt_helper.fire_event('on_process_finalising', self)

    def on_finish(self, retval):
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

    @classmethod
    def build(cls, func, output_name="value"):
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
                    {'_define': staticmethod(_define),
                     '_func': func,
                     '_func_args': args,
                     '_output_name': output_name})

    def __init__(self):
        super(FunctionProcess, self).__init__()

    def _run(self, **kwargs):
        args = []
        for arg in self._func_args:
            args.append(kwargs.pop(arg))

        self._out(self._output_name, self._func(*args))


