# -*- coding: utf-8 -*-

from abc import ABCMeta
import collections
import logging
 
_LOGGER = logging.getLogger(__name__)


class ValueSpec(object):
    """
    Specifications relating to a general input/output value including
    properties like whether it is required, valid types, the help string, etc.
    """
    __metaclass__ = ABCMeta

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
                return False, "required value was not provided for '{}'".\
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
                assert (len(result) == 2)
                return result
            elif result is False:
                return False, "Value failed validation"

        return True, None


class Attribute(ValueSpec):
    def __init__(self, name, valid_type=None, help=None, default=None, required=False):
        super(Attribute, self).__init__(name, valid_type=valid_type,
                                        help=help, required=required)
        self._default = default

    @property
    def default(self):
        return self._default


class Port(ValueSpec):
    __metaclass__ = ABCMeta
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
        if default is None:
            return required
        else:
            return False

    def __init__(self, name, valid_type=None, help=None, default=None,
                 required=True, validator=None):
        super(InputPort, self).__init__(
            name, valid_type=valid_type, help=help, required=InputPort.required_override(required, default),
            validator=validator)
 
        if required is not InputPort.required_override(required, default):
            _LOGGER.info("the required attribute for the input port '{}' was overridden "
                "because a default was specified".format(name))

        if default is not None:
            default_valid, msg = self.validate(default)
            if not default_valid:
                raise ValueError("Invalid default value: {}".format(msg))

        self._default = default

    def __str__(self):
        desc = [super(InputPort, self).__str__()]
        if self.default:
            desc.append(str(self.default))

        return "->" + ",".join(desc)

    @property
    def default(self):
        return self._default


class InputGroupPort(InputPort):
    def __init__(self, name, valid_type=None, help=None, default=None,
                 required=False):
        # We have to set _valid_inner_type before calling the super constructor
        # because it will call validate on the default value (if supplied)
        # which in turn needs this value to be set.
        if default is not None and not isinstance(default, dict):
            raise ValueError("Input group default must be of type dict")
        self._valid_inner_type = valid_type

        super(InputGroupPort, self).__init__(name, valid_type=dict,
                                             help=help,
                                             default=default, required=required)

    @property
    def default(self):
        return self._default

    def validate(self, value):
        valid, msg = super(InputGroupPort, self).validate(value)
        if not valid:
            return False, msg

        if value is not None and self._valid_inner_type is not None:
            # Check that all the members of the dictionary are of the right type
            for k, v in value.iteritems():
                if not isinstance(v, self._valid_inner_type):
                    return False, "Group port value {} is not of the right type".format(
                        k)

        return True, None


class DynamicInputPort(InputPort):
    """
    A dynamic output port represents the fact that a Process can emit outputs
    that weren't defined beforehand
    """
    NAME = "dynamic"

    def __init__(self, valid_type=None, help_=None):
        super(DynamicInputPort, self).__init__(
            self.NAME, valid_type=valid_type, help=help_, default=None,
            required=False)


class OutputPort(Port):
    def __init__(self, name, valid_type=None, required=True):
        super(OutputPort, self).__init__(name, valid_type)
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
