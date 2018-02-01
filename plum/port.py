# -*- coding: utf-8 -*-
import json
import collections
import logging
from abc import ABCMeta
from copy import deepcopy
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
        return '; '.join(self.get_description())

    def get_description(self):
        """
        Return a description of the ValueSpec, which will be a dictionary of its attributes

        :returns: a dictionary of the stringified ValueSpec attributes
        """
        description = {
            'name': '{}'.format(self.name)
        }

        if self.valid_type:
            description['valid_type'] = '{}'.format(self.valid_type)
        if self.required:
            description['required'] = '{}'.format(self.required)
        if self.help:
            description['help'] = '{}'.format(self.help.strip())

        return description

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def valid_type(self):
        return self._valid_type

    @valid_type.setter
    def valid_type(self, valid_type):
        self._valid_type = valid_type

    @property
    def help(self):
        return self._help

    @help.setter
    def help(self, help):
        self._help = help

    @property
    def required(self):
        return self._required

    @required.setter
    def required(self, required):
        self._required = required

    @property
    def validator(self):
        return self._validator

    @validator.setter
    def validator(self, validator):
        self._validator = validator

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

    def has_default(self):
        return self._default is not _NULL

    @property
    def default(self):
        if not self.has_default():
            raise RuntimeError('No default')
        return self._default

    @default.setter
    def default(self, default):
        self._default = default

    def get_description(self):
        """
        Return a description of the InputPort, which will be a dictionary of its attributes

        :returns: a dictionary of the stringified InputPort attributes
        """
        description = super(InputPort, self).get_description()

        if self.has_default():
            description['default'] = '{}'.format(self.default)

        return description


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

    def __init__(self, name=None, help=None, required=True, validator=None, valid_type=None, default=_NULL, dynamic=False):
        super(PortNamespace, self).__init__(
            name=name, help=help, required=required, validator=validator, valid_type=valid_type
        )
        self._ports = {}
        self._default = default
        self._dynamic = dynamic

    def __str__(self):
        return json.dumps(self.get_description(), sort_keys=True, indent=4)

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
            raise TypeError('port needs to be an instance of Port')
        self._ports[key] = port

    @property
    def ports(self):
        return self._ports

    def has_default(self):
        return self._default is not _NULL

    @property
    def default(self):
        return self._default

    @default.setter
    def default(self, default):
        self._default = default

    @property
    def dynamic(self):
        return self._dynamic

    @dynamic.setter
    def dynamic(self, dynamic):
        self._dynamic = dynamic

    def get_description(self):
        """
        Return a dictionary with a description of the ports this namespace contains
        Nested PortNamespaces will be properly recursed and Ports will print their properties in a list

        :returns: a dictionary of descriptions of the Ports contained within this PortNamespace
        """
        description = {
            '_attrs': {
                'default': self.default,
                'dynamic': self.dynamic,
                'valid_type': str(self.valid_type),
            }
        }

        for name, port in self._ports.items():
            description[name] = port.get_description()

        return description

    def get_port(self, name=None):
        """
        Retrieve a (nested) port namespace within this port namespace. Nested port namespaces
        that do not exist will be created

        :param name: namespace of the port to retrieve, if None returns self
        :returns: Port
        """
        if name is None:
            return self

        namespace = name.split(self.NAMESPACE_SEPARATOR)
        port_name = namespace.pop(0)

        if port_name not in self:
            self[port_name] = self.__class__(port_name)

        if namespace:
            return self[port_name].get_port(self.NAMESPACE_SEPARATOR.join(namespace))
        else:
            return self[port_name]

    def absorb(self, port_namespace, exclude=(), include=None):
        """
        Absorb another PortNamespace instance into oneself, including all its attributes and ports.
        Attributes of self will be overwritten with those of the port namespace that is to be absorbed.
        The same goes for the ports, meaning that any ports with a key that already exists in self will
        be overwritten. The attributes and ports of the port namespace that is to be absorbed are deep copied.
        The exclude and include tuples can be used to exclude or include certain ports. Both are mutually exclusive.

        :param port_namespace: instance of PortNamespace that is to be absorbed into self
        :param exclude: list or tuple of input keys to exclude from being exposed
        :param include: list or tuple of input keys to include as exposed inputs
        """
        if not isinstance(port_namespace, PortNamespace):
            raise ValueError('port_namespace has to be an instance of PortNamespace')

        absorb_attrs = deepcopy(port_namespace.__dict__)
        absorb_ports = absorb_attrs.pop('_ports', {})

        # Override all attributes except for the ports collection
        self.__dict__.update(absorb_attrs)

        for name, port in absorb_ports.items():

            if include is not None:
                if name not in include:
                    continue
            else:
                if name in exclude:
                    continue

            self[name] = port

    def project(self, port_values):
        """
        Project a (nested) dictionary of port values onto the port dictionary of this PortNamespace. That is
        to say, return those keys of the dictionary that are shared by this PortNamespace. If a matching key
        corresponds to another PortNamespace, this method will be called recursively, passing the sub dictionary
        belonging to that port name.

        :param port_values: a dictionary where keys are port names and values are actual input values
        """
        result = {}

        for name, value in port_values.items():
            if name in self.ports:
                if isinstance(value, PortNamespace):
                    result[name] = self[name].project(value)
                else:
                    result[name] = value

        return result

    def validate(self, port_values=None):
        """
        Validate the namespace port itself and subsequently all the port_values it contains

        :param port_values: an arbitrarily nested dictionary of parsed port values
        """
        is_valid = True
        message = None

        if port_values is None:
            port_values = {}
        else:
            port_values = dict(port_values)

        # Validate the validator first as it most likely will rely on the port values
        if self._validator is not None:
            is_valid, message = self._validator(self, port_values)
            if not is_valid:
                return is_valid, message

        # Validate each port individually, popping its name if found in input dictionary
        for name, port in self._ports.items():
            is_valid, message = port.validate(port_values.pop(name, None))
            if not is_valid:
                return is_valid, message

        # If any port_values remain, we better support dynamic ports
        if port_values and not self.dynamic:
            is_valid = False
            message = 'Unexpected ports {}, for a non dynamic namespace'.format(port_values)

        # If any port_values remain and we have a valid_type, make sure they match the type
        if port_values and self._valid_type is not None:
            valid_type = self._valid_type
            for port_name, port_value in port_values.items():
                if not isinstance(port_value, valid_type):
                    is_valid = False
                    message = 'Invalid type {} for dynamic port value: expected {}'.format(
                        type(port_value), valid_type)

        return is_valid, message