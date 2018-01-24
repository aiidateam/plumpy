# -*- coding: utf-8 -*-

import collections
import logging
from abc import ABCMeta
from future.utils import with_metaclass


_LOGGER = logging.getLogger(__name__)
_NULL = ()


class ValueSpec(with_metaclass(ABCMeta, object)):
    """
    Specifications relating to a general input/output value including
    properties like whether it is required, valid types, the help string, etc.
    """

    def __init__(self, name, valid_type=None, help=None, required=True, validator=None):
        self._name = name
        self._valid_type = valid_type
        self._help = help
        self._required = required
        self._validator = validator

    def __str__(self):
        return self.get_description()

    def get_description(self):
        desc = ["{}".format(self.name)]
        if self.valid_type:
            desc.append("valid type(s): {}".format(self.valid_type))
        if self.help:
            desc.append("help: {}".format(self.help))
        if self.required:
            desc.append("required: {}".format(self.required))
        return ", ".join(desc)

    @property
    def name(self):
        return self._name

    @property
    def valid_type(self):
        return self._valid_type

    @property
    def help(self):
        return self._help

    @property
    def required(self):
        return self._required

    @property
    def validator(self):
        return self._validator

    def validate(self, value):
        if value is None:
            if self._required:
                return False, "required value was not provided for '{}'". \
                    format(self.name)
        else:
            if self._valid_type is not None and \
                    not isinstance(value, self._valid_type):
                msg = "value '{}' is not of the right type. " \
                      "Got '{}', expected '{}'".format(
                    self.name, type(value), self._valid_type)
                return False, msg

        if self._validator is not None:
            result = self._validator(value)
            if isinstance(result, collections.Sequence):
                assert (len(result) == 2), "Invalid validator return type"
                return result
            elif result is False:
                return False, "Value failed validation"

        return True, None


class Port(with_metaclass(ABCMeta, ValueSpec)):
    pass


class InputPort(Port):
    """
    A simple input port for a value being received by a process
    """

    @staticmethod
    def required_override(required, default):
        """
        If a default is specified an input should no longer be marked
        as required. Otherwise the input should always be marked explicitly
        to be not required even if a default is specified.
        """
        if default is _NULL:
            return required
        else:
            return False

    def __init__(self, name, valid_type=None, help=None, default=_NULL, required=True, validator=None):
        super(InputPort, self).__init__(
            name, valid_type=valid_type, help=help, required=InputPort.required_override(required, default),
            validator=validator)

        if required is not InputPort.required_override(required, default):
            _LOGGER.info("the required attribute for the input port '{}' was overridden "
                         "because a default was specified".format(name))

        if default is not _NULL:
            default_valid, msg = self.validate(default)
            if not default_valid:
                raise ValueError("Invalid default value: {}".format(msg))

        self._default = default

    def __str__(self):
        desc = [super(InputPort, self).__str__()]
        if self.default:
            desc.append(str(self.default))

        return "->" + ",".join(desc)

    def has_default(self):
        return self._default is not _NULL

    @property
    def default(self):
        if not self.has_default():
            raise RuntimeError("No default")
        return self._default


class InputGroupPort(InputPort):
    """
    An input group, this corresponds to a mapping where if validation is used
    then each value is checked to meet the validation criteria rather than
    the whole input itself.
    """

    def __init__(self, name, valid_type=None, help=None, default=_NULL,
                 required=False):
        # We have to set _valid_inner_type before calling the super constructor
        # because it will call validate on the default value (if supplied)
        # which in turn needs this value to be set.
        if default is not _NULL and not isinstance(default, collections.Mapping):
            raise ValueError("Input group default must be of type Mapping")
        self._valid_inner_type = valid_type

        super(InputGroupPort, self).__init__(
            name, valid_type=dict, help=help, default=default,
            required=required)

    @property
    def default(self):
        return self._default

    def validate(self, value):
        valid, msg = super(InputGroupPort, self).validate(value)
        if not valid:
            return False, msg

        if value is not None and self._valid_inner_type is not None:
            # Check that all the members of the dictionary are of the right type
            for k, v in value.items():
                if not isinstance(v, self._valid_inner_type):
                    return False, "Group port value {} is not of the right type. Should be of type {}, but is {}.".format(
                        k, self._valid_inner_type, type(v))

        return True, None


class OutputPort(Port):
    def __init__(self, name, valid_type=None, required=True, help=None):
        super(OutputPort, self).__init__(name, valid_type, help=help)
        self._required = required


class PortNamespace(collections.MutableMapping, Port):
    """
    A container for Ports. Effectively it maintains a dictionary whose members are
    either a Port or yet another PortNamespace. This allows for the nesting of ports
    """
    NAMESPACE_SEPARATOR = '.'

    def __init__(self, namespace=None, help=None, required=True, validator=None, valid_type=None):
        super(PortNamespace, self).__init__(
            name=namespace, help=help, required=required, validator=validator, valid_type=valid_type
        )
        self._ports = {}
        self._dynamic = False

    @property
    def is_dynamic(self):
        return self._dynamic

    def set_dynamic(self, dynamic):
        self._dynamic = dynamic

    def set_validator(self, validator):
        self._validator = validator

    def set_valid_type(self, valid_type):
        self._valid_type = valid_type

    @property
    def ports(self):
        return self._ports

    def __iter__(self):
        return self._ports.__iter__()

    def __len__(self):
        return len(self._ports)

    def __delitem__(self, key):
        del self._ports[key]

    def __getitem__(self, key):
        return self._ports[key]

    def __setitem__(self, key, port):
        if not isinstance(port, Port):
            raise ValueError('port needs to be an instance of Port')
        self._ports[key] = port

    def add_port(self, port, name):
        """
        Add a port, optionally within a (nested) namespace

        :param port: the port to add
        :param name: key or namespace under which to store the port
        """
        namespace = name.split(self.NAMESPACE_SEPARATOR)
        port_name = namespace.pop(0)

        if port_name not in self:
            self[port_name] = PortNamespace(port_name)

        if namespace:
            self[port_name].add_port(port, self.NAMESPACE_SEPARATOR.join(namespace))
        else:
            self[port_name] = port

    def get_port(self, name):
        """
        Retrieve a port, optionally within a (nested) namespace

        :param name: key or namespace of the port to retrieve
        :returns: Port
        """
        namespace = name.split(self.NAMESPACE_SEPARATOR)
        port_name = namespace.pop(0)

        port = self[port_name]

        if namespace:
            return port.get_port(self.NAMESPACE_SEPARATOR.join(namespace))
        else:
            return port

    def add_port_namespace(self, name):
        """
        Add a port namespace

        :param name: key or namespace under which to store the port
        """
        namespace = name.split(self.NAMESPACE_SEPARATOR)
        port_name = namespace.pop(0)

        if port_name not in self:
            self[port_name] = PortNamespace(port_name)

        if namespace:
            self[port_name].add_port_namespace(self.NAMESPACE_SEPARATOR.join(namespace))

    def validate(self, port_values=None):
        """
        Validate the namespace port itself and subsequently all the ports it contains

        :param ports: an arbitrarily nested dictionary of parsed port values
        """
        is_valid = True
        message = None

        if port_values is None:
            ports = {}
        else:
            ports = dict(port_values)

        # Validate the validator first as it most likely will rely on the port values
        if self._validator is not None:
            is_valid, message = self._validator(self, ports)
            if not is_valid:
                return is_valid, message

        # Validate each port individually, popping its name if found in input dictionary
        for name, port in self._ports.items():
            is_valid, message = port.validate(ports.pop(name, None))
            if not is_valid:
                return is_valid, message

        # If any ports remain, we better support dynamic ports
        if ports and not self.is_dynamic:
            is_valid = False
            message = 'Unexpected ports {}, for a non dynamic namespace'.format(ports)

        # If any ports remain and we have a valid_type, make sure they match the type
        if ports and self._valid_type is not None:
            valid_type = self._valid_type
            for port_name, port_value in ports.items():
                if not isinstance(port_value, valid_type):
                    is_valid = False
                    message = 'Invalid type {} for dynamic port value: expected {}'.format(
                        type(port_value), valid_type)

        return is_valid, message