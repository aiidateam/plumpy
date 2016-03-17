# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
import plum.util as util
from plum.port import InputPort, OutputPort


class ProcessSpec(object):
    def __init__(self):
        self._inputs = {}
        self._outputs = {}
        self._sealed = False

    #
    # @classmethod
    # def init(cls):
    #     pass
    #
    # @classmethod
    # def is_initialising(cls):
    #     try:
    #         return cls._initialising
    #     except AttributeError:
    #         return False
    #
    # @classmethod
    # def is_initialised(cls):
    #     try:
    #         return cls._initialised
    #     except AttributeError:
    #         return False
    #
    # @classmethod
    # def ensure_initialised(cls):
    #     if not cls.is_initialising() and not cls.is_initialised():
    #         cls._initialising = True
    #         cls._inputs = {}
    #         cls._outputs = {}
    #         cls.init()
    #         cls._initialised = True

    def seal(self):
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

    def add_input(self, name, **kwargs):
        if self.sealed:
            raise RuntimeError("Cannot add an input after spec is sealed")

        if name in self._inputs:
            raise ValueError("Input {} already exists.".format(name))

        port = InputPort(self, name, **kwargs)
        self._inputs[name] = port
        return port

    def remove_input(self, name):
        if self.sealed:
            raise RuntimeError("Cannot remove an input after spec is sealed")
        self._inputs.pop(name)

    def add_output(self, name, **kwargs):
        if self.sealed:
            raise RuntimeError("Cannot add an output after spec is sealed")
        if name in self._outputs:
            raise ValueError("Output {} already exists.".format(name))

        self._outputs[name] = OutputPort(self, name, **kwargs)

    def remove_output(self, name):
        if self.sealed:
            raise RuntimeError("Cannot remove an input after spec is sealed")
        self._outputs.pop(name)


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

    @classmethod
    def get_name(cls):
        return cls.__name__

    def bind(self, input_port, value):
        """
        Convenience method to push a value to a particular input port.

        :param input_port: The input port to bind the value to.
        :param value: The input value.
        """
        self._input_values[input_port] = value

    def ready_to_run(self):
        for name, port in self.spec().inputs.iteritems():
            if name not in self._input_values and port.default is None:
                return False

        return True

    def run(self):
        self._check_inputs()

        # Fill out the arguments
        ins = self._create_input_args()

        self._on_process_starting(ins)

        outs = self._run(**ins)
        self._check_outputs(outs)

        self._on_process_finished(outs)

        return outs

    def _create_input_args(self):
        kwargs = {}
        for name, port in self.spec().inputs.iteritems():
            kwargs[name] = self._input_values.pop(name, port.default)

        return kwargs

    def _check_inputs(self):
        # Check all the input ports are filled
        for name, port in self.spec().inputs.iteritems():
            if name not in self._input_values and port.default is None:
                raise RuntimeError(
                    "Cannot run process {} because port {}"
                    " is not filled".format(self.get_name(), name))

    def _check_outputs(self, out):
        # Do checks on the outputs
        try:
            # Check number
            if len(out) != len(self.spec().outputs):
                raise ValueError(
                    "Process should produce {} outputs, got {}".
                        format(len(self.spec().outputs), len(out)))

            # Check types (if known)
            for key, value in out.iteritems():
                port = self.spec().get_output(key)
                if port.type is not None and not isinstance(value, port.type):
                    raise TypeError(
                        "Process returned output {} of wrong type, expected "
                        "{}".format(key, port.type))
        except AttributeError:
            raise TypeError("Return value of a process must be a mapping")

    @abstractmethod
    def _run(self, **kwargs):
        pass

    def _on_process_starting(self, inputs):
        pass

    def _on_process_finished(self, outputs):
        pass


def _crazy_init(cls, spec):
    for i in range(len(args)):
        if defaults and len(defaults) - len(args) + i >= 0:
            spec.add_input(args[i], default=defaults[i])
        else:
            spec.add_input(args[i])

    spec.add_output(output_name)

class FunctionProcess(Process):
    @staticmethod
    def build(func, output_name="value"):
        import inspect

        args, varargs, keywords, defaults = inspect.getargspec(func)

        def init(spec):
            for i in range(len(args)):
                if defaults and len(defaults) - len(args) + i >= 0:
                    spec.add_input(args[i], default=defaults[i])
                else:
                    spec.add_input(args[i])

            spec.add_output(output_name)

        func_proc = type(func.__name__, (FunctionProcess,),
                         {'_func': func,
                          '_init': abstractmethod(init),
                          '_func_args': args,
                          '_output_name': output_name})
        return func_proc

    def __init__(self):
        super(FunctionProcess, self).__init__()

    def _run(self, **kwargs):
        args = []
        for arg in self._func_args:
            args.append(kwargs.pop(arg))
        return {self._output_name: self._func(*args)}
