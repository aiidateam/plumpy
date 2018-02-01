# -*- coding: utf-8 -*-
import json
import logging
from collections import defaultdict
from plum.port import Port, PortNamespace, InputPort, OutputPort


class ProcessSpec(object):
    """
    A class that defines the specifications of a :class:`plum.process.Process`,
    this includes what its inputs, outputs, etc are.

    All methods to modify the spec should have declarative names describe the
    spec e.g.: input, output

    Every Process class has one of these.
    """
    NAME_INPUTS_PORT_NAMESPACE = 'inputs'
    NAME_OUTPUTS_PORT_NAMESPACE = 'outputs'
    PORT_NAMESPACE_TYPE = PortNamespace
    INPUT_PORT_TYPE = InputPort
    OUTPUT_PORT_TYPE = OutputPort


    def __init__(self):
        self._ports = self.PORT_NAMESPACE_TYPE()
        self._validator = None
        self._sealed = False
        self._logger = logging.getLogger(__name__)

        # Create the input and output port namespace
        self._ports.create_port_namespace(self.NAME_INPUTS_PORT_NAMESPACE)
        self._ports.create_port_namespace(self.NAME_OUTPUTS_PORT_NAMESPACE)

    def __str__(self):
        return json.dumps(self.get_description(), sort_keys=True, indent=4)

    @property
    def namespace_separator(self):
        return self.PORT_NAMESPACE_TYPE.NAMESPACE_SEPARATOR

    @property
    def logger(self):
        return self._logger

    def seal(self):
        """
        Seal this specification disallowing any further changes
        """
        self._sealed = True

    @property
    def sealed(self):
        """
        Indicates if the spec is sealed or not

        :return: True if sealed, False otherwise
        :rtype: bool
        """
        return self._sealed

    def get_description(self):
        """
        Get a description of this process specification

        :return: a list with dictionaries of descriptions of input and output ports if defined
        """
        description = []
        if self.inputs:
            description.append({'inputs': self.inputs.get_description()})

        if self.outputs:
            description.append({'outputs': self.outputs.get_description()})

        return description

    @property
    def ports(self):
        return self._ports

    @property
    def inputs(self):
        """
        Get the input port namespace of the process specification

        :return: the input PortNamespace
        """
        return self._ports[self.NAME_INPUTS_PORT_NAMESPACE]

    @property
    def outputs(self):
        """
        Get the output port namespace of the process specification

        :return: the outputs PortNamespace
        """
        return self._ports[self.NAME_OUTPUTS_PORT_NAMESPACE]

    def _create_port(self, port_namespace, port_class, name, **kwargs):
        """
        Create a new Port of a given class and name in a given PortNamespace

        :param port_namespace: PortNamespace to which to add the port
        :param port_class: class of the Port to create
        :param name: name of the port to create
        :param kwargs: options for the port
        """
        if self.sealed:
            raise RuntimeError('Cannot add an output port after the spec has been sealed')

        namespace = name.split(self.namespace_separator)
        port_name = namespace.pop(0)

        if namespace:
            namespace = self.namespace_separator.join(namespace)
            port_namespace = port_namespace.create_port_namespace(namespace)

        port_namespace[port_name] = port_class(port_name, **kwargs)

    def input(self, name, **kwargs):
        """
        Define an input port in the input port namespace

        :param name: name of the input port to create
        :param kwargs: options for the input port
        """
        self._create_port(self.inputs, self.INPUT_PORT_TYPE, name, **kwargs)

    def output(self, name, **kwargs):
        """
        Define an output port in the output port namespace

        :param name: name of the output port to create
        :param kwargs: options for the output port
        """
        self._create_port(self.outputs, self.OUTPUT_PORT_TYPE, name, **kwargs)

    def input_namespace(self, name, **kwargs):
        """
        Create a new PortNamespace in the input port namespace. The keyword arguments will be
        passed to the PortNamespace constructor. Any intermediate port namespaces that need to
        be created for a nested namespace, will take constructor defaults

        :param name: namespace of the new port namespace
        :param kwargs: keyword arguments for the PortNamespace constructor
        """
        self._create_port(self.inputs, self.PORT_NAMESPACE_TYPE, name, **kwargs)

    def output_namespace(self, name, **kwargs):
        """
        Create a new PortNamespace in the output port namespace. The keyword arguments will be
        passed to the PortNamespace constructor. Any intermediate port namespaces that need to
        be created for a nested namespace, will take constructor defaults

        :param name: namespace of the new port namespace
        :param kwargs: keyword arguments for the PortNamespace constructor
        """
        self._create_port(self.outputs, self.PORT_NAMESPACE_TYPE, name, **kwargs)

    def has_input(self, name):
        """
        Return whether the input port namespace contains a port with the given name

        :param name: key of the port in the input port namespace
        """
        return name in self.inputs

    def has_output(self, name):
        """
        Return whether the output port namespace contains a port with the given name

        :param name: key of the port in the output port namespace
        """
        return name in self.outputs

    def dynamic_input(self, valid_type=None):
        """
        Make the input port namespace dynamic and optional set a valid_type for the inputs

        :param valid_type: a single or tuple of valid input types
        """
        self.inputs.dynamic = True
        self.inputs.valid_type = valid_type

    def dynamic_output(self, valid_type=None):
        """
        Make the output port namespace dynamic and optional set a valid_type for the outputs

        :param valid_type: a single or tuple of valid output types
        """
        self.outputs.dynamic = True
        self.outputs.valid_type = valid_type

    def no_dynamic_input(self):
        """
        Remove the dynamic property from the inputs port namespace
        """
        self.inputs.dynamic = False

    def no_dynamic_output(self):
        """
        Remove the dynamic property from the outputs port namespace
        """
        self.outputs.dynamic = False

    def has_dynamic_input(self):
        """
        Return whether the input port namespace is dynamic
        """
        return self.inputs.dynamic

    def has_dynamic_output(self):
        """
        Return whether the output port namespace is dynamic
        """
        return self.outputs.dynamic

    def inputs_validator(self, validator):
        """
        Supply a validator function for the output port namespace. The function signature should
        takes two arguments: spec and inputs, where spec will be this specification and inputs
        will be a dictionary of inputs to be validated. It should return a tuple of (bool, str|None),
        where the bool indicates if the inputs are valid and the string is an optional error message.

        :param validator: the validation function
        :return: valid or not, error string|None
        :rtype: tuple(bool, str|None)
        """
        self.inputs.validator = validator

    def outputs_validator(self, validator):
        """
        Supply a validator function for the output port namespace. The function signature should
        takes two arguments: spec and outputs, where spec will be this specification and outputs
        will be a dictionary of outputs to be validated. It should return a tuple of (bool, str|None),
        where the bool indicates if the outputs are valid and the string is an optional error message.

        :param validator: the validation function
        :return: valid or not, error string|None
        :rtype: tuple(bool, str|None)
        """
        self.outputs.validator = validator

    def validate_inputs(self, inputs=None):
        """
        Validate a dictionary of inputs according to the input port namespace of this specification

        :param inputs: the inputs dictionary
        :return: valid or not, error string|None
        :rtype: tuple(bool, str or None)
        """
        return self.inputs.validate(inputs)

    def validate_outputs(self, outputs=None):
        """
        Validate a dictionary of outputs according to the output port namespace of this specification

        :param outputs: the outputs dictionary
        :return: valid or not, error string|None
        :rtype: tuple(bool, str or None)
        """
        return self.outputs.validate(outputs)

    def expose_inputs(self, process_class, namespace=None, exclude=(), include=None):
        """
        This method allows one to automatically add the inputs from another Process to this ProcessSpec.
        The optional namespace argument can be used to group the exposed inputs in a separated PortNamespace.
        Specific input ports from the exposed process can be excluded or included, but they are mutually exclusive
        and only one can be specified at a time.

        :param process_class: the Process class whose inputs to expose
        :param namespace: a namespace in which to place the exposed inputs
        :param exclude: list or tuple of input keys to exclude from being exposed
        :param include: list or tuple of input keys to include as exposed inputs
        """
        if exclude and include is not None:
            raise ValueError('exclude and include are mutually exclusive')

        if namespace is None:
            port_namespace = self.inputs
        else:
            port_namespace = self.inputs.create_port_namespace(namespace)

        port_namespace.absorb(process_class.spec().inputs, exclude, include)

    def exposed_inputs(self, inputs, process_class, namespace=None):
        """
        Return a dictionary of inputs that were exposed for a given Process class under an optional namespace.
        The exposed inputs dictionary will effectively be obtained by projecting the inputs dictionary on the
        input port namespace of the process class

        :param inputs: the dictionary of validated inputs passed to the Process
        :param process_class: process class whose inputs to try and retrieve
        :param namespace: optional sub PortNamespace in which to look for the inputs
        """
        if namespace is not None:
            inputs = inputs[namespace]

        port_namespace = process_class.spec().inputs
        project_inputs = port_namespace.project(inputs)

        return project_inputs
