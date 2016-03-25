# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
from plum.port import InputPort, OutputPort, DynamicOutputPort
import plum.util as util


class ProcessSpec(object):
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

    def add_input(self, name, **kwargs):
        self.add_input_port(name, InputPort(self, name, **kwargs))

    def add_input_port(self, name, port):
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

    def add_output(self, name, **kwargs):
        self.add_output_port(name, OutputPort(self, name, **kwargs))

    def add_output_port(self, name, port):
        if self.sealed:
            raise RuntimeError("Cannot add an output after spec is sealed")
        if not isinstance(port, OutputPort):
            raise TypeError("Output port must be an instance of OutputPort")
        if name in self._outputs:
            raise ValueError("Output {} already exists.".format(name))

        self._outputs[name] = port

    def add_dynamic_output(self):
        self.add_output_port(DynamicOutputPort.NAME, DynamicOutputPort(self))

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

    def on_process_finished(self, process, retval):
        pass

    def on_input_bound(self, process, input_port, value):
        pass

    def on_output_emitted(self, process, output_port, value, dynamic):
        pass


class Process(object):
    __metaclass__ = ABCMeta

    # Static class stuff ######################
    _spec_type = ProcessSpec

    @classmethod
    def spec(cls):
        try:
            return cls._spec
        except AttributeError:
            cls._spec = cls._spec_type()
            cls._init(cls._spec)
            return cls._spec

    @classmethod
    def create(cls):
        return cls()

    @staticmethod
    def _init(spec):
        pass

    ############################################

    def __init__(self):
        # Don't allow the spec to be changed anymore
        self.spec().seal()
        self._input_values = {}
        self._output_values = {}
        self._proc_evt_helper = util.EventHelper(ProcessListener)

    def __call__(self, **kwargs):
        for k, v in kwargs.iteritems():
            self.bind(k, v)
        return self.run()

    @classmethod
    def get_name(cls):
        return cls.__name__

    def add_process_listener(self, listener):
        assert (listener != self)
        self._proc_evt_helper.add_listener(listener)

    def remove_process_listener(self, listener):
        self._proc_evt_helper.remove_listener(listener)

    def bind(self, input_port, value):
        """
        Convenience method to push a value to a particular input port.

        :param input_port: The input port to bind the value to.
        :param value: The input value.
        """
        self._input_values[input_port] = value

    def is_input_bound(self, input_port):
        return input_port in self._input_values

    def can_run(self):
        for name, port in self.spec().inputs.iteritems():
            if port.required and name not in self._input_values:
                return False

        return True

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

    def run(self):
        self._check_inputs()
        self._output_values = {}

        # Fill out the arguments
        ins = self._create_input_args()
        # Now reset the input arguments
        self._input_values = {}

        self._on_process_starting(ins)

        retval = self._run(**ins)
        self._check_outputs()

        self._on_process_finished(retval)

        return retval

    def get_last_outputs(self):
        return self._output_values

    def _create_input_args(self):
        kwargs = {}
        for name, port in self.spec().inputs.iteritems():
            if name in self._input_values:
                kwargs[name] = self._input_values[name]
            elif port.default:
                kwargs[name] = port.default
            else:
                assert (not port.required)

        return kwargs

    def _check_inputs(self):
        # Check all the input ports are filled
        for name, port in self.spec().inputs.iteritems():
            if name not in self._input_values and port.default is None:
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

    def _on_process_starting(self, inputs):
        self._proc_evt_helper.fire_event('on_process_starting',
                                         self, inputs)

    def _on_process_finished(self, retval):
        self._proc_evt_helper.fire_event('on_process_finished',
                                         self, retval)

    def _on_input_bound(self, input_port, value):
        self._proc_evt_helper.fire_event('on_input_bound',
                                         self, input_port, value)

    def _on_output_emitted(self, output_port, value, dynamic):
        self._proc_evt_helper.fire_event('on_output_emitted',
                                         self, output_port, value, dynamic)


class FunctionProcess(Process):
    @staticmethod
    def build(func, output_name="value"):
        import inspect

        args, varargs, keywords, defaults = inspect.getargspec(func)

        def init(spec):
            for i in range(len(args)):
                default = None
                if defaults and len(defaults) - len(args) + i >= 0:
                    default = defaults[i]
                spec.add_input(args[i], default=default)

            spec.add_output(output_name)

        return type(func.__name__, (FunctionProcess,),
                    {'_func': func,
                     '_init': staticmethod(init),
                     '_func_args': args,
                     '_output_name': output_name})

    def __init__(self):
        super(FunctionProcess, self).__init__()

    def _run(self, **kwargs):
        args = []
        for arg in self._func_args:
            args.append(kwargs.pop(arg))

        self.out(self._output_name, self._func(*args))


