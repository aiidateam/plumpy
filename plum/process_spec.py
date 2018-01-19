# -*- coding: utf-8 -*-
import logging
from collections import defaultdict
from plum.port import InputPort, InputGroupPort, OutputPort, \
    DynamicOutputPort, DynamicInputPort, PortNamespace

LOGGER = logging.getLogger(__name__)


class ProcessSpec(object):
    """
    A class that defines the specifications of a :class:`plum.process.Process`,
    this includes what its inputs, outputs, etc are.

    All methods to modify the spec should have declarative names describe the
    spec e.g.: input, output

    Every Process class has one of these.
    """
    INPUT_PORT_TYPE = InputPort
    DYNAMIC_INPUT_PORT_TYPE = DynamicInputPort
    INPUT_GROUP_PORT_TYPE = InputGroupPort

    def __init__(self):
        self._inputs = PortNamespace()
        self._outputs = PortNamespace()
        self._exposed_inputs = defaultdict(lambda: defaultdict(list))
        self._validator = None
        self._sealed = False

    def seal(self):
        """
        Seal this specification disallowing any further changes.
        """
        self._sealed = True

    @property
    def sealed(self):
        """
        Indicates if the spec is sealed or not.

        :return: True if sealed, False otherwise
        :rtype: bool
        """
        return self._sealed

    def get_description(self):
        """
        Get a text description of this process specification.

        :return: A text description
        :rtype: str
        """
        desc = []
        if self.inputs:
            desc.append("Inputs")
            desc.append("======")
            desc.append("".join([p.get_description() for k, p in
                                 sorted(self.inputs.items(),
                                        key=lambda x: x[0])]))

        if self.outputs:
            desc.append("Outputs")
            desc.append("=======")
            desc.append("".join([p.get_description() for k, p in
                                 sorted(self.outputs.items(),
                                        key=lambda x: x[0])]))

        return "\n".join(desc)

    # region Inputs
    @property
    def inputs(self):
        """
        Get the inputs of the process specification

        :return: The inputs
        :rtype: dict
        """
        return self._inputs

    def get_input(self, name):
        try:
            return self._inputs[name]
        except KeyError:
            raise ValueError("Unknown input '{}'".format(name))

    def get_dynamic_input(self):
        return self._inputs.get(DynamicInputPort.NAME, None)

    def has_input(self, name):
        return name in self._inputs

    def input(self, name, **kwargs):
        """
        Define an Process input.

        :param name: The name of the input.
        :param kwargs: The input port options.
        """
        self.input_port(name, self.INPUT_PORT_TYPE(name, **kwargs))

    def dynamic_input(self, **kwargs):
        self.input_port(self.DYNAMIC_INPUT_PORT_TYPE.NAME,
                        self.DYNAMIC_INPUT_PORT_TYPE(**kwargs))

    def no_dynamic_input(self):
        try:
            self.remove_input(DynamicInputPort.NAME)
        except KeyError:
            pass

    def has_dynamic_input(self):
        return self.has_input(DynamicInputPort.NAME)

    def input_group(self, name, **kwargs):
        self.input_port(name, self.INPUT_GROUP_PORT_TYPE(name, **kwargs))

    def input_port(self, name, port):
        if self.sealed:
            raise RuntimeError("Cannot add an input after spec is sealed")
        if not isinstance(port, InputPort):
            raise TypeError("Input port must be an instance of InputPort")
        if name in self._inputs:
            LOGGER.info("Overwriting existing input '{}'.".format(name))

        self._inputs[name] = port

    def remove_input(self, name):
        if self.sealed:
            raise RuntimeError("Cannot remove an input after spec is sealed")
        self._inputs.pop(name)

    # endregion

    # region Outputs
    @property
    def outputs(self):
        return self._outputs

    def get_output(self, name):
        return self._outputs[name]

    def get_dynamic_output(self):
        return self._outputs.get(DynamicOutputPort.NAME, None)

    def has_output(self, name):
        return name in self._outputs

    def has_dynamic_output(self):
        return self.has_output(DynamicOutputPort.NAME)

    def output(self, name, **kwargs):
        self.output_port(name, OutputPort(name, **kwargs))

    def optional_output(self, name, **kwargs):
        self.output_port(name, OutputPort(name, required=False, **kwargs))

    def output_port(self, name, port):
        if self.sealed:
            raise RuntimeError("Cannot add an output after spec is sealed")
        if not isinstance(port, OutputPort):
            raise TypeError("Output port must be an instance of OutputPort")
        if name in self._outputs:
            LOGGER.info("Overwriting existing output '{}'.".format(name))

        self._outputs[name] = port

    def dynamic_output(self, **kwargs):
        self.output_port(
            DynamicOutputPort.NAME, DynamicOutputPort(**kwargs))

    def no_dynamic_output(self):
        try:
            self.remove_output(DynamicOutputPort.NAME)
        except KeyError:
            pass

    def remove_output(self, name):
        if self.sealed:
            raise RuntimeError("Cannot remove an input after spec is sealed")
        self._outputs.pop(name)

    # end region

    def validator(self, fn):
        """
        Supply a validator function.  This should be a function that takes two
        arguments: spec and inputs where spec will be this specification and
        inputs will be a dictionary of inputs to be validated.  It should
        return a tuple of bool, str|None where the bool indicates if the inputs
        are valid and the str can optionally be used to provide a message with
        a description of the problems(s) or it can be None.

        :param fn: The validation function
        :return: valid or not, error string|None
        :rtype: tuple(bool, str|None)
        """
        self._validator = fn

    def validate(self, inputs=None):
        """
        This will validate a dictionary of inputs to make sure they are valid
        according to this specification.

        :param inputs: The inputs dictionary
        :type inputs: dict
        :return: A tuple indicating if the input is valid or not and an
            optional error message
        :rtype: tuple(bool, str or None)
        """
        if inputs is None:
            inputs = {}

        # Check the inputs meet the requirements
        if not self.has_dynamic_input():
            unexpected = set(inputs.keys()) - set(self.inputs.keys())
            if unexpected:
                return False, \
                       "Unexpected inputs found: '{}'.  If you want to allow " \
                       "dynamic inputs add dynamic_input() to the spec " \
                       "definition.".format(unexpected)

        for name, port in self.inputs.items():
            valid, msg = port.validate(inputs.get(name, None))
            if not valid:
                return False, msg

        if self._validator is not None:
            valid, msg = self._validator(self, inputs)
            if not valid:
                return False, msg

        return True, None

    def expose_inputs(self, process_class, namespace=None, exclude=(), include=None):
        """
        This method allows one to automatically add the inputs from another
        Process to this ProcessSpec. The optional namespace argument can be
        used to group the exposed inputs in a separated PortNamespace

        :param process_class: the Process class whose inputs to expose
        :param namespace: a namespace in which to place the exposed inputs
        :param exclude: list or tuple of input keys to exclude from being exposed
        """
        if exclude and include is not None:
            raise ValueError('exclude and include are mutually exclusive')

        if namespace:
            self._inputs[namespace] = PortNamespace(namespace)
            port_namespace = self._inputs[namespace]
        else:
            port_namespace = self._inputs

        exposed_inputs_list = self._exposed_inputs[namespace][process_class]

        for name, port in process_class.spec().inputs.iteritems():

            if name.startswith('_') or name == 'dynamic':
                continue

            if include is not None:
                if name not in include:
                    continue
            else:
                if name in exclude:
                    continue

            port_namespace[name] = port
            exposed_inputs_list.append(name)
