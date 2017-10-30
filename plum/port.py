# -*- coding: utf-8 -*-

from abc import ABCMeta
import collections
import logging

from future.utils import raise_from, with_metaclass

from .exceptions import ValidationError

_LOGGER = logging.getLogger(__name__)

_NULL = ()


class ValueSpec(with_metaclass(ABCMeta, object)):
    """
    Specifications relating to a general input/output value including
    properties like whether it is required, valid types, the help string, etc.
    """

    def __init__(self, name, valid_type=None, help=None, required=True,
                 validator=None):
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
                raise ValidationError(
                    "required value was not provided for '{}'".format(self.name)
                )
        else:
            if not self._check_type(value):
                raise ValidationError(
                    "value '{}' is not of the right type. Got '{}', expected '{}'".format(
                        self.name, type(value), self._valid_type
                    )
                )

        if self._validator is not None:
            result = self._validator(value)
            if isinstance(result, collections.Sequence):
                assert (len(result) == 2), "Invalid validator return type"
                valid, msg = result
                if not valid:
                    raise ValidationError(msg)
            elif result is False:
                raise ValidationError("Value failed validation")

    def _check_type(self, value):
        return self._valid_type is None or isinstance(value, self._valid_type)


class Attribute(ValueSpec):
    def __init__(self, name, valid_type=None, help=None, default=_NULL, required=False):
        super(Attribute, self).__init__(name, valid_type=valid_type,
                                        help=help, required=required)
        self._default = default

    @property
    def default(self):
        return self._default


class Port(with_metaclass(ABCMeta, ValueSpec)):
    pass


class InputPort(Port):
    """
    A simple input port for a value being received by a workflow.
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

    def __init__(self, name, valid_type=None, help=None, default=_NULL,
                 required=True, validator=None, serialize_fct=None):
        super(InputPort, self).__init__(
            name, valid_type=valid_type, help=help, required=InputPort.required_override(required, default),
            validator=validator)

        if required is not InputPort.required_override(required, default):
            _LOGGER.info("the required attribute for the input port '{}' was overridden "
                         "because a default was specified".format(name))

        if default is not _NULL:
            try:
                self.validate(default)
            except ValidationError as err:
                raise_from(err, ValidationError("Invalid default value: {}".format(err)))

        self._default = default
        self._serialize_fct = serialize_fct

    def evaluate(self, value):
        value = self._serialize(value)
        self.validate(value)
        return value

    def _serialize(self, value):
        if not self._check_type(value):
            value = self._serialize_fct(value)
        return value

    def __str__(self):
        desc = [super(InputPort, self).__str__()]
        if self.default:
            desc.append(str(self.default))

        return "->" + ",".join(desc)

    @property
    def default(self):
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
        super(InputGroupPort, self).validate(value)

        if value is not None and self._valid_inner_type is not None:
            # Check that all the members of the dictionary are of the right type
            for k, v in value.items():
                if not isinstance(v, self._valid_inner_type):
                    raise ValidationError(
                        "Group port value {} is not of the right type. Should be of type {}, but is {}.".format(
                            k, self._valid_inner_type, type(v)
                        )
                    )

class DynamicInputPort(InputPort):
    """
    A dynamic output port represents the fact that a Process can emit outputs
    that weren't defined beforehand
    """
    NAME = "dynamic"

    def __init__(self, valid_type=None, help_=None):
        super(DynamicInputPort, self).__init__(
            self.NAME, valid_type=valid_type, help=help_, required=False)


class OutputPort(Port):
    def __init__(self, name, valid_type=None, required=True, help=None):
        super(OutputPort, self).__init__(name, valid_type, help=help)
        self._required = required

    @property
    def required(self):
        return self._required


class DynamicOutputPort(OutputPort):
    """
    A dynamic output port represents the fact that a Process can emit outputs
    that weren't defined beforehand
    """
    NAME = "dynamic"

    def __init__(self, valid_type=None):
        super(DynamicOutputPort, self).__init__(
            self.NAME, valid_type=valid_type, required=False)
