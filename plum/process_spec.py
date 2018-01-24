# -*- coding: utf-8 -*-
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
        self._exposed_inputs = defaultdict(lambda: defaultdict(list))
        self._validator = None
        self._sealed = False
        self._logger = logging.getLogger(__name__)

        # Create the input and output port namespace
        self._ports.add_port_namespace(self.NAME_INPUTS_PORT_NAMESPACE)
        self._ports.add_port_namespace(self.NAME_OUTPUTS_PORT_NAMESPACE)

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
        Get a text description of this process specification

        :return: A text description
        :rtype: str
        """
        description = []
        if self.inputs:
            description.append("Inputs")
            description.append("======")
            description.append("".join([p.get_description() for k, p in
                                 sorted(self.inputs.items(),
                                        key=lambda x: x[0])]))

        if self.outputs:
            description.append("Outputs")
            description.append("=======")
            description.append("".join([p.get_description() for k, p in
                                 sorted(self.outputs.items(),
                                        key=lambda x: x[0])]))

        return "\n".join(description)

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

    def create_port(self, port_namespace, port_class, name, **kwargs):
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
        port_name = namespace[-1]
        port = port_class(port_name, **kwargs)
        port_namespace.add_port(port, self.namespace_separator.join(namespace))

    def input(self, name, **kwargs):
        """
        Define an input port in the input port namespace

        :param name: name of the input port to create
        :param kwargs: options for the input port
        """
        self.create_port(self.inputs, self.INPUT_PORT_TYPE, name, **kwargs)

    def output(self, name, **kwargs):
        """
        Define an output port in the output port namespace

        :param name: name of the output port to create
        :param kwargs: options for the output port
        """
        self.create_port(self.outputs, self.OUTPUT_PORT_TYPE, name, **kwargs)

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
        self.inputs.set_dynamic(True)
        self.inputs.set_valid_type(valid_type)

    def dynamic_output(self, valid_type=None):
        """
        Make the output port namespace dynamic and optional set a valid_type for the outputs

        :param valid_type: a single or tuple of valid output types
        """
        self.outputs.set_dynamic(True)
        self.outputs.set_valid_type(valid_type)

    def no_dynamic_input(self):
        """
        Remove the dynamic property from the inputs port namespace
        """
        self.inputs.set_dynamic(False)

    def no_dynamic_output(self):
        """
        Remove the dynamic property from the outputs port namespace
        """
        self.outputs.set_dynamic(False)

    def has_dynamic_input(self):
        """
        Return whether the input port namespace is dynamic
        """
        return self.inputs.is_dynamic

    def has_dynamic_output(self):
        """
        Return whether the output port namespace is dynamic
        """
        return self.outputs.is_dynamic

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
        self.inputs.set_validator(validator)

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
        self.outputs.set_validator(validator)

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
            self.inputs[namespace] = self.PORT_NAMESPACE_TYPE(namespace)
            port_namespace = self.inputs[namespace]
        else:
            port_namespace = self.inputs

        exposed_inputs_list = self._exposed_inputs[namespace][process_class]

        input_ports = process_class.spec().inputs

        # If the inputs namespace of process class' spec is dynamic, inherit it
        if input_ports.is_dynamic:
            port_namespace.set_dynamic(True)

        for name, port in input_ports.items():

            if include is not None:
                if name not in include:
                    continue
            else:
                if name in exclude:
                    continue

            port_namespace[name] = port
            exposed_inputs_list.append(name)