# -*- coding: utf-8 -*-

from abc import ABCMeta
import collections


class ValueSpec(object):
    __metaclass__ = ABCMeta

    def __init__(self, process, name, valid_type=None, help=None,
                 required=True, validator=None):
        self._name = name
        self._process = process
        self._valid_type = valid_type
        self._help = help
        self._required = required
        self._validator = validator

    @property
    def name(self):
        return self._name

    @property
    def process(self):
        return self._process

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
                return False, "Required value was not provided"
        else:
            if self._valid_type is not None and \
                    not isinstance(value, self._valid_type):
                return False, "parameter '{}' is not of the right type. " \
                              "Got '{}', expected '{}'".format(
                    self.name, type(value), self._valid_type)

        if self._validator is not None:
            result = self._validator(value)
            if isinstance(result, collections.Sequence):
                assert (len(result) == 2)
                return result
            elif result is False:
                return False, "Value failed validation"

        return True, None


class Attribute(ValueSpec):
    def __init__(self, process, name, valid_type=None, help=None, default=None,
                 required=False):
        super(Attribute, self).__init__(process, name, valid_type=valid_type,
                                        help=help, required=required)
        self._default = default

    def __str__(self):
        my_desc = ["=>", "name: {}".format(self.name)]
        if self._valid_type:
            my_desc.append("type: {}".format(self._valid_type))
        if self._default:
            my_desc.append("default: {}".format(self._default))
        return ", ".join(my_desc)

    @property
    def default(self):
        return self._default


class Port(ValueSpec):
    __metaclass__ = ABCMeta
    pass


class InputPort(Port):
    def __init__(self, process, name, valid_type=None, help=None, default=None,
                 required=True, validator=None):
        super(InputPort, self).__init__(
            process, name, valid_type=valid_type, help=help, required=required,
            validator=validator)
        self._default = default

    def __str__(self):
        my_desc = ["=>", "name: {}".format(self.name)]
        if self._valid_type:
            my_desc.append("type: {}".format(self._valid_type))
        if self._default:
            my_desc.append("default: {}".format(self._default))
        return ", ".join(my_desc)

    @property
    def default(self):
        return self._default


class InputGroupPort(InputPort):
    def __init__(self, process, name, valid_type=None, help=None, default=None,
                 required=False):
        super(InputGroupPort, self).__init__(process, name, valid_type=dict,
                                             help=help,
                                             default=default, required=required)

        if default is not None and not isinstance(default, dict):
            raise ValueError("Input group default must be of type dict")
        self._valid_inner_type = valid_type

    def __str__(self):
        my_desc = ["=>", "name: {}".format(self.name)]
        if self._valid_type:
            my_desc.append("type: {}".format(self._valid_type))
        if self._default:
            my_desc.append("default: {}".format(self._default))
        return ", ".join(my_desc)

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

    def __init__(self, process, valid_type=None, help_=None):
        super(DynamicInputPort, self).__init__(
            process, self.NAME, valid_type=valid_type, help=help_, default=None,
            required=False)


class OutputPort(Port):
    def __init__(self, process, name, valid_type=None, required=True):
        super(OutputPort, self).__init__(process, name, valid_type)
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

    def __init__(self, process, valid_type=None):
        super(DynamicOutputPort, self).__init__(
            process, self.NAME, valid_type=valid_type, required=False)
