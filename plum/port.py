# -*- coding: utf-8 -*-

from abc import ABCMeta
import plum.util as util


class Port(object):
    __metaclass__ = ABCMeta

    def __init__(self, process, name, valid_type=None, help=None, required=True):
        self._name = name
        self._process = process
        self._valid_type = valid_type
        self._help = help
        self._required = required

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

    def validate(self, value):
        if value is None:
            if self._required:
                return False, "Required port value was not provided"
        else:
            if self._valid_type is not None and not isinstance(value, self._valid_type):
                return False, "Port value is not of the right kind"

        return True, None


class InputPort(Port):
    def __init__(self, process, name, valid_type=None, help=None, default=None,
                 required=True):
        super(InputPort, self).__init__(process, name, valid_type=valid_type,
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


class InputGroupPort(InputPort):
    def __init__(self, process, name, valid_type=None, help=None, default=None,
                 required=False):
        super(InputGroupPort, self).__init__(process, name, valid_type=dict, help=help,
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
            for k, v in value:
                if not isinstance(v, self._valid_inner_type):
                    return False, "Group port value {} is not of the right type".format(k)

        return True, None


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

    def __init__(self, process):
        super(DynamicOutputPort, self).__init__(
            process, self.NAME, dict, False)
