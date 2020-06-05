# -*- coding: utf-8 -*-
"""Module for process ports"""
import collections
import copy
import inspect
import json
import logging
import warnings

from plumpy.utils import is_mutable_property, type_check

__all__ = ['UNSPECIFIED', 'PortValidationError', 'Port', 'InputPort', 'OutputPort']

_LOGGER = logging.getLogger(__name__)
UNSPECIFIED = ()

VALIDATOR_SIGNATURE_DEPRECATION_WARNING = """the validator `{}` has a signature that only takes a single argument.
    This has been deprecated and the new signature is `validator(value, port)` where the `port` argument will be the
    port instance to which the validator has been assigned."""


class PortValidationError(Exception):
    """Error when validation fails on a port"""

    def __init__(self, message, port):
        """
        :param message: validation error message
        :type message: str
        :param port: the port where the validation error occurred
        :type: str
        """
        super().__init__("Error occurred validating port '{}': {}".format(port, message))
        self._message = message
        self._port = port

    @property
    def message(self):
        """
        Get the validation error message

        :return: the error message
        :rtype: str
        """
        return self._message

    @property
    def port(self):
        """
        Get the port breadcrumbs

        :return: the port where the error occurred
        :rtype: str
        """
        return self._port


class Port:
    """
    Specifications relating to a general input/output value including
    properties like whether it is required, valid types, the help string, etc.
    """

    def __init__(self, name, valid_type=None, help=None, required=True, validator=None):  # pylint: disable=redefined-builtin
        self._name = name
        self._valid_type = valid_type
        self._help = help
        self._required = required
        self._validator = validator

    def __str__(self):
        """Get the string representing this port.

        :return: the string representation
        :rtype: str
        """
        return json.dumps(self.get_description())

    def get_description(self):
        """Return a description of the Port, which will be a dictionary of its attributes

        :returns: a dictionary of the stringified Port attributes
        :rtype: dict
        """
        description = {
            'name': '{}'.format(self.name),
            'required': str(self.required),
        }

        if self.valid_type:
            description['valid_type'] = '{}'.format(self.valid_type)
        if self.help:
            description['help'] = '{}'.format(self.help.strip())

        return description

    @property
    def name(self):
        return self._name

    @property
    def valid_type(self):
        """Get the valid value type for this port if one is specified

        :return: the value value type
        :rtype: type
        """
        return self._valid_type

    @valid_type.setter
    def valid_type(self, valid_type):
        """Set the valid value type for this port

        :param valid_type: the value valid type
        :type valid_type: type
        """
        self._valid_type = valid_type

    @property
    def help(self):
        """Get the help string for this port

        :return: the help string
        :rtype: str
        """
        return self._help

    @help.setter
    def help(self, help):  # pylint: disable=redefined-builtin
        """Set the help string for this port

        :param help: the help string
        :type help: str
        """
        self._help = help

    @property
    def required(self):
        """Is this port required?

        :return: True if required, False otherwise
        :rtype: bool
        """
        return self._required

    @required.setter
    def required(self, required):
        """Set whether this port is required or not

        :return: True if required, False otherwise
        :type required: bool
        """
        self._required = required

    @property
    def validator(self):
        """Get the validator for this port

        :return: the validator
        :rtype: typing.Callable[[typing.Any], typing.Tuple[bool, typing.Optional[str]]]
        """
        return self._validator

    @validator.setter
    def validator(self, validator):
        """Set the validator for this port

        :param validator: a validator function
        :type validator: typing.Callable[[typing.Any], typing.Tuple[bool, typing.Optional[str]]]
        """
        self._validator = validator

    def validate(self, value, breadcrumbs=()):
        """Validate a value to see if it is valid for this port

        :param value: the value to check
        :type value: typing.Any
        :param breadcrumbs: a tuple of the path to having reached this point in validation
        :type breadcrumbs: typing.Tuple[str]
        :return: None or tuple containing 0: error string 1: tuple of breadcrumb strings to where the validation failed
        :rtype: typing.Optional[ValidationError]
        """
        validation_error = None

        if value is UNSPECIFIED and self._required:
            validation_error = "required value was not provided for '{}'".format(self.name)
        elif value is not UNSPECIFIED and self._valid_type is not None and not isinstance(value, self._valid_type):
            validation_error = "value '{}' is not of the right type. Got '{}', expected '{}'".format(
                self.name, type(value), self._valid_type
            )

        if not validation_error and self._validator is not None:
            spec = inspect.getfullargspec(self.validator)
            if len(spec[0]) == 1:
                warnings.warn(VALIDATOR_SIGNATURE_DEPRECATION_WARNING.format(self.validator.__name__))
                result = self.validator(value)  # pylint: disable=not-callable
            else:
                result = self.validator(value, self)  # pylint: disable=not-callable
            if result is not None:
                assert isinstance(result, str), 'Validator returned non string type'
                validation_error = result

        if validation_error:
            breadcrumbs += (self.name,)
            return PortValidationError(validation_error, breadcrumbs_to_port(breadcrumbs))


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
        if default is UNSPECIFIED:
            return required

        return False

    def __init__(self, name, valid_type=None, help=None, default=UNSPECIFIED, required=True, validator=None):  # pylint: disable=redefined-builtin,too-many-arguments
        super().__init__(
            name,
            valid_type=valid_type,
            help=help,
            required=InputPort.required_override(required, default),
            validator=validator
        )

        if required is not InputPort.required_override(required, default):
            _LOGGER.info(
                "the required attribute for the input port '%s' was overridden because a default was specified", name
            )

        if default is not UNSPECIFIED:

            # Only validate the default value if it is not a callable. If it is a callable its return value will always
            # be validated when the port is validated upon process construction, if the default is was actually used.
            if not callable(default):
                validation_error = self.validate(default)
                if validation_error:
                    raise ValueError('Invalid default value: {}'.format(validation_error.message))

        self._default = default

    def has_default(self):
        return self._default is not UNSPECIFIED

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
        description = super().get_description()

        if self.has_default():
            description['default'] = '{}'.format(self.default)

        return description


class OutputPort(Port):
    pass


class PortNamespace(collections.abc.MutableMapping, Port):
    """
    A container for Ports. Effectively it maintains a dictionary whose members are
    either a Port or yet another PortNamespace. This allows for the nesting of ports
    """

    NAMESPACE_SEPARATOR = '.'

    def __init__(
        self,
        name=None,
        help=None,
        required=True,
        validator=None,
        valid_type=None,
        default=UNSPECIFIED,
        dynamic=False,
        populate_defaults=True
    ):  # pylint: disable=redefined-builtin,too-many-arguments
        """Construct a port namespace.

        :param name: the name of the namespace
        :param help: the help string
        :param required: boolean, if `True` the validation will fail if no value is specified for this namespace
        :param validator: an optional validator for the namespace
        :param valid_type: optional tuple of valid types in the case of a dynamic namespace. Setting this to anything
            other than `None` will automatically force `dynamic` to be set to `True`.
        :param default: default value for the port
        :param dynamic: boolean, if `True`, the namespace will accept values even when no explicit port is defined
        :param populate_defaults: boolean, when set to `False`, the populating of defaults for this namespace is skipped
            entirely, including all nested namespaces, if no explicit value is passed for this port in the parent
            namespace. As soon as a value is specified in the parent namespace for this port, even if it is empty, this
            property is ignored and the population of defaults is always performed.
        """
        super().__init__(name=name, help=help, required=required, validator=validator, valid_type=valid_type)
        self._ports = {}
        self.default = default
        self.populate_defaults = populate_defaults
        self.valid_type = valid_type

        # Do not override `dynamic` if the `valid_type` is not `None` because the setter will have set it properly
        if valid_type is None:
            self.dynamic = dynamic

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
        return self._default is not UNSPECIFIED

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

    @property
    def valid_type(self):
        return super().valid_type

    @valid_type.setter
    def valid_type(self, valid_type):
        """Set the `valid_type` for the `PortNamespace`.

        If the `valid_type` is None, the `dynamic` property will be set to `False`, in all other cases `dynamic` will be
        set to True.

        :param valid_type: a tuple or single valid type that the namespace accepts
        """
        if valid_type is None:
            self.dynamic = False
        else:
            self.dynamic = True

        super(PortNamespace, self.__class__).valid_type.fset(self, valid_type)  # pylint: disable=no-member

    @property
    def populate_defaults(self):
        return self._populate_defaults

    @populate_defaults.setter
    def populate_defaults(self, populate_defaults):
        self._populate_defaults = populate_defaults

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
                'required': str(self.required),
                'help': self.help,
            }
        }

        for name, port in self._ports.items():
            description[name] = port.get_description()

        return description

    def get_port(self, name):
        """
        Retrieve a (namespaced) port from this PortNamespace. If any of the sub namespaces of the terminal
        port itself cannot be found, a ValueError will be raised

        :param name: name (potentially namespaced) of the port to retrieve
        :returns: Port
        :raises: ValueError if port or namespace does not exist
        """
        if not isinstance(name, str):
            raise ValueError('name has to be a string type, not {}'.format(type(name)))

        if not name:
            raise ValueError('name cannot be an empty string')

        namespace = name.split(self.NAMESPACE_SEPARATOR)
        port_name = namespace.pop(0)

        if port_name not in self:
            raise ValueError("port '{}' does not exist in port namespace '{}'".format(port_name, self.name))

        if namespace:
            return self[port_name].get_port(self.NAMESPACE_SEPARATOR.join(namespace))

        return self[port_name]

    def create_port_namespace(self, name, **kwargs):
        """
        Create and return a new PortNamespace in this PortNamespace. If the name is namespaced, the sub PortNamespaces
        will be created recursively, except if one of the namespaces is already occupied at any level by
        a Port in which case a ValueError will be thrown

        :param name: name (potentially namespaced) of the port to create and return
        :param kwargs: constructor arguments that will be used *only* for the construction of the terminal PortNamespace
        :returns: PortNamespace
        :raises: ValueError if any sub namespace is occupied by a non-PortNamespace port
        """
        if not isinstance(name, str):
            raise ValueError('name has to be a string type, not {}'.format(type(name)))

        if not name:
            raise ValueError('name cannot be an empty string')

        namespace = name.split(self.NAMESPACE_SEPARATOR)
        port_name = namespace.pop(0)

        if port_name in self and not isinstance(self[port_name], PortNamespace):
            raise ValueError("the name '{}' in '{}' already contains a Port".format(port_name, self.name))

        # If this is True, the (sub) port namespace does not yet exist, so we create it
        if port_name not in self:

            # If there still is a `namespace`, we create a sub namespace, *without* the constructor arguments
            if namespace:
                self[port_name] = self.__class__(port_name)

            # Otherwise it is the terminal port and we construct *with* the keyword arugments
            else:
                self[port_name] = self.__class__(port_name, **kwargs)

        if namespace:
            return self[port_name].create_port_namespace(self.NAMESPACE_SEPARATOR.join(namespace), **kwargs)

        return self[port_name]

    def absorb(self, port_namespace, exclude=None, include=None, namespace_options=None):
        """Absorb another PortNamespace instance into oneself, including all its mutable properties and ports.

        Mutable properties of self will be overwritten with those of the port namespace that is to be absorbed.
        The same goes for the ports, meaning that any ports with a key that already exists in self will
        be overwritten. The namespace_options dictionary can be used to yet override the mutable properties of
        the port namespace that is to be absorbed. The exclude and include tuples can be used to exclude or
        include certain ports and both are mutually exclusive.

        :param port_namespace: instance of PortNamespace that is to be absorbed into self
        :param exclude: list or tuple of input keys to exclude from being exposed
        :param include: list or tuple of input keys to include as exposed inputs
        :param namespace_options: a dictionary with mutable PortNamespace property values to override
        :return: list of the names of the ports that were absorbed
        """
        # pylint: disable=too-many-branches
        if not isinstance(port_namespace, PortNamespace):
            raise ValueError('port_namespace has to be an instance of PortNamespace')

        if exclude is not None and include is not None:
            raise ValueError('exclude and include are mutually exclusive')

        if exclude is not None:
            type_check(exclude, (list, tuple))
        elif include is not None:
            type_check(include, (list, tuple))

        if namespace_options is None:
            namespace_options = {}

        # Overload mutable attributes of PortNamespace unless overridden by value in namespace_options
        for attr in dir(port_namespace):
            if is_mutable_property(PortNamespace, attr):
                if attr in namespace_options:
                    setattr(self, attr, namespace_options.pop(attr))
                else:
                    setattr(self, attr, getattr(port_namespace, attr))

        if namespace_options:
            raise ValueError(
                'the namespace_options {}, is not a supported PortNamespace property'.format(
                    ', '.join(list(namespace_options.keys()))
                )
            )

        absorbed_ports = []

        for port_name, port in port_namespace.items():

            # If the current port name occurs in the exclude list, simply skip it entirely, there is no need to consider
            # any of the nested ports it might have, even if it is a port namespace
            if exclude and port_name in exclude:
                continue

            if isinstance(port, PortNamespace):

                # If the name does not appear at the start of any of the include rules we continue:
                if include and not any([rule.startswith(port_name) for rule in include]):
                    continue

                # Determine the sub exclude and include rules for this specific namespace
                sub_exclude = self.strip_namespace(port_name, self.NAMESPACE_SEPARATOR, exclude)
                sub_include = self.strip_namespace(port_name, self.NAMESPACE_SEPARATOR, include)

                # Create a new namespace at `port_name` and copy the original port namespace itself such that we keep
                # all its mutable properties, but reset its ports, since those will be taken care of by the recursive
                # absorb call that will properly consider the include and exclude rules
                self[port_name] = copy.copy(port)
                self[port_name]._ports = {}  # pylint: disable=protected-access
                self[port_name].absorb(port, sub_exclude, sub_include)
            else:
                # If include rules are specified but the port name does not appear, simply skip it
                if include and port_name not in include:
                    continue

                self[port_name] = copy.deepcopy(port)

            absorbed_ports.append(port_name)

        return absorbed_ports

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

    def validate(self, port_values=None, breadcrumbs=()):  # pylint: disable=arguments-differ
        """
        Validate the namespace port itself and subsequently all the port_values it contains

        :param port_values: an arbitrarily nested dictionary of parsed port values
        :type port_values: dict
        :param breadcrumbs: a tuple of the path to having reached this point in validation
        :type breadcrumbs: typing.Tuple[str]
        :return: None or tuple containing 0: error string 1: tuple of breadcrumb strings to where the validation failed
        :rtype: typing.Optional[ValidationError]
        """
        breadcrumbs_local = breadcrumbs + (self.name,)

        if not port_values:
            port_values = {}

        if not isinstance(port_values, collections.abc.Mapping):
            message = 'specified value is of type {} which is not sub class of `Mapping`'.format(type(port_values))
            return PortValidationError(message, breadcrumbs_to_port(breadcrumbs_local))

        # Turn the port values into a normal dictionary and create a shallow copy. The `validate_ports` method to which
        # the `port_values` will be passed, will pop the values corresponding to explicit ports. This is necessary for
        # the `validate_dynamic_ports` to correctly detect any implicit ports. However, the `validator` will expect the
        # entire original input namespace to be there. Since the latter has to be called after the ports have been
        # validated, we need to create a clone of the port values here.
        port_values = dict(port_values)
        port_values_clone = port_values.copy()

        # If the namespace is not required and there are no port_values specified, consider it valid
        if not port_values and not self.required:
            return

        # In all other cases, validate all input ports explicitly specified in this port namespace
        validation_error = self.validate_ports(port_values, breadcrumbs_local)
        if validation_error:
            return validation_error

        # If any port_values remain, validate against the dynamic properties of the namespace
        validation_error = self.validate_dynamic_ports(port_values, breadcrumbs)
        if validation_error:
            return validation_error

        # Validate the validator after the ports themselves, as it most likely will rely on the port values
        if self.validator is not None:
            spec = inspect.getfullargspec(self.validator)
            if len(spec[0]) == 1:
                warnings.warn(VALIDATOR_SIGNATURE_DEPRECATION_WARNING.format(self.validator.__name__))
                message = self.validator(port_values_clone)  # pylint: disable=not-callable
            else:
                message = self.validator(port_values_clone, self)  # pylint: disable=not-callable
            if message is not None:
                assert isinstance(message, str), \
                    "Validator returned something other than None or str: '{}'".format(type(message))
                return PortValidationError(message, breadcrumbs_to_port(breadcrumbs_local))

    def pre_process(self, port_values):
        """Map port values onto the port namespace, filling in values for ports with a default.

        :param port_values: the dictionary with supplied port values
        :return: an AttributesFrozenDict with pre-processed port value mapping, complemented with port default values
        """
        from . import utils

        for name, port in self.items():

            # If the port was not specified in the inputs values and the port is a namespace with the property
            # `populate_defaults=False`, we skip the pre-processing and do not populate defaults.
            if name not in port_values and isinstance(port, PortNamespace) and not port.populate_defaults:
                continue

            if name not in port_values:

                if port.has_default():
                    default = port.default
                    if callable(default):
                        port_value = default()
                    else:
                        port_value = default

                # If a namespace containing ports, create an empty dictionary so its ports can be considered recursively
                elif isinstance(port, PortNamespace) and port.ports:
                    port_value = {}
                else:
                    continue
            else:
                port_value = port_values[name]

            if isinstance(port, PortNamespace):
                port_values[name] = port.pre_process(port_value)
            else:
                port_values[name] = port_value

        return utils.AttributesFrozendict(port_values)

    def validate_ports(self, port_values, breadcrumbs):
        """
        Validate port values with respect to the explicitly defined ports of the port namespace.
        Ports values that are matched to an actual Port will be popped from the dictionary

        :param port_values: an arbitrarily nested dictionary of parsed port values
        :type port_values: dict
        :param breadcrumbs: a tuple of breadcrumbs showing the path to to the value being validated
        :type breadcrumbs: tuple
        :return: None or tuple containing 0: error string 1: tuple of breadcrumb strings to where the validation failed
        :rtype: typing.Optional[ValidationError]
        """
        for name, port in self._ports.items():
            validation_error = port.validate(port_values.pop(name, UNSPECIFIED), breadcrumbs)
            if validation_error:
                return validation_error

    def validate_dynamic_ports(self, port_values, breadcrumbs=()):
        """
        Validate port values with respect to the dynamic properties of the port namespace. It will
        check if the namespace is actually dynamic and if all values adhere to the valid types of
        the namespace if those are specified

        :param port_values: an arbitrarily nested dictionary of parsed port values
        :type port_values: dict
        :param breadcrumbs: a tuple of the path to having reached this point in validation
        :type breadcrumbs: typing.Tuple[str]
        :return: if invalid returns a string with the reason for the validation failure, otherwise None
        :rtype: typing.Optional[str]
        """
        breadcrumbs += (self.name,)

        if port_values and not self.dynamic:
            msg = 'Unexpected ports {}, for a non dynamic namespace'.format(port_values)
            return PortValidationError(msg, breadcrumbs_to_port(breadcrumbs))

        if self.valid_type is not None:
            valid_type = self.valid_type
            for port_name, port_value in port_values.items():
                if not isinstance(port_value, valid_type):
                    msg = 'Invalid type {} for dynamic port value: expected {}'.format(type(port_value), valid_type)
                    return PortValidationError(msg, breadcrumbs_to_port(breadcrumbs + (port_name,)))

    @staticmethod
    def strip_namespace(namespace, separator, rules=None):
        """Filter given exclude/include rules staring with namespace and strip the first level.

        For example if the namespace is `base` and the rules are::

            ('base.a', 'base.sub.b','relax.base.c', 'd')

        the function will return::

            ('a', 'sub.c')

        If the rules are `None`, that is what is returned as well.

        :param namespace: the string name of the namespace
        :param separator: the namespace separator string
        :param rules: the list or tuple of exclude or include rules to strip
        :return: `None` if `rules=None` or the list of stripped rules
        """
        if rules is None:
            return rules

        stripped = []

        prefix = '{}{}'.format(namespace, separator)

        for rule in rules:
            if rule.startswith(prefix):
                stripped.append(rule[len(prefix):])

        return stripped


def breadcrumbs_to_port(breadcrumbs):
    """Convert breadcrumbs to a string representing the port

    :param breadcrumbs: a tuple of the path to the port
    :type breadcrumbs: typing.Tuple[str]
    :return: the port string
    :rtype: str
    """
    return PortNamespace.NAMESPACE_SEPARATOR.join(breadcrumbs)
