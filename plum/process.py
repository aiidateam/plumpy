# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
import plum.util as util
import plum.event as event
from plum.port import InputPort, OutputPort


class Process(object):
    __metaclass__ = ABCMeta

    def __init__(self, name):
        self._process_events = util.EventHelper(event.ProcessListener)
        self.name = name
        self._inputs = {}
        self._outputs = {}

    def add_process_listener(self, listener):
        self._process_events.add_listener(listener)

    def remove_process_listener(self, listener):
        self._process_events.remove_listener(listener)

    def get_inputs(self):
        return self._inputs

    def get_input(self, name):
        return self._inputs[name]

    def get_outputs(self):
        return self._outputs

    def get_output(self, name):
        return self._outputs[name]

    def add_input(self, name, **kwargs):
        if name in self._inputs:
            raise ValueError("Input {} already exists.".format(name))

        port = InputPort(self, name, *kwargs)
        self._inputs[name] = port
        return port

    def remove_input(self, name):
        self._inputs.pop(name)

    def add_output(self, name, **kwargs):
        if name in self._outputs:
            raise ValueError("Output {} already exists.".format(name))

        self._outputs[name] = OutputPort(self, name, *kwargs)

    def remove_output(self, name):
        self._outputs.pop(name)

    def bind(self, input_port, value):
        """
        Convenience method to push a value to a particular input port.

        :param input_port: The input port to bind the value to.
        :param value: The input value.
        """
        self.get_input(input_port).push(value)

    def ready_to_run(self):
        for port in self.get_inputs().itervalues():
            if not port.is_filled() and port.default is None:
                return False

        return True

    def run(self):
        # Check all the input ports are filled
        for port in self.get_inputs().itervalues():
            if not port.is_filled() and port.default is None:
                raise RuntimeError(
                    "Cannot run process {} because port {}"
                    " is not filled".format(self.name, port.name))

        # Fill out the arguments
        kwargs = {}
        for port in self.get_inputs().itervalues():
            if port.is_filled():
                kwargs[port.name] = port.pop()
            else:
                kwargs[port.name] = port.default

        for listener in self._process_events.listeners:
            listener.process_starting(self)

        out = self._run(**kwargs)

        # Do checks on the outputs
        try:
            # Check number
            if len(out) != len(self.get_outputs()):
                raise ValueError(
                    "Process should produce {} outputs, got {}".
                    format(len(self.get_outputs()), len(out)))

            # Check types (if known)
            for key, value in out.iteritems():
                port = self.get_output(key)
                if port.type is not None and not isinstance(value, port.type):
                    raise TypeError(
                        "Process returned output {} of wrong type, expected "
                        "{}".format(key, port.type))
        except AttributeError:
            raise TypeError("Return value of a process must be a mapping")

        for listener in self._process_events.listeners:
            listener.process_finished(self, out)

        return out

    @abstractmethod
    def _run(self, **kwargs):
        pass


class FunctionProcess(Process):
    def __init__(self, func, output_name="value"):
        import inspect

        super(FunctionProcess, self).__init__(func.__name__)

        args, varargs, keywords, defaults = inspect.getargspec(func)

        for i in range(len(args)):
            if defaults and len(defaults) - len(args) + i >= 0:
                self.add_input(args[i], default=defaults[i])
            else:
                self.add_input(args[i])

        self.func_args = args
        self.output_name = output_name
        self.add_output(output_name)
        self.func = func

    def _run(self, **kwargs):
        args = []
        for arg in self.func_args:
            args.append(kwargs.pop(arg))
        return {self.output_name: self.func(*args)}
