# -*- coding: utf-8 -*-

from abc import ABCMeta
import plum.util as util


class Port(object):
    __metaclass__ = ABCMeta

    def __init__(self, process, name, type=None, help=None, required=True):
        self._name = name
        self._process = process
        self._type = type
        self._help = help
        self._required = required

    @property
    def name(self):
        return self._name

    @property
    def process(self):
        return self._process

    @property
    def type(self):
        return self._type

    @property
    def help(self):
        return self._help

    @property
    def required(self):
        return self._required


class InputPort(Port):
    def __init__(self, process, name, type=None, default=None):
        super(InputPort, self).__init__(process, name, type)
        self._default = default

    def __str__(self):
        my_desc = ["=>", "name: {}".format(self.name)]
        if self._type:
            my_desc.append("type: {}".format(self._type))
        if self._default:
            my_desc.append("default: {}".format(self._default))
        return ", ".join(my_desc)

    @property
    def default(self):
        return self._default


class OutputPort(Port):
    def __init__(self, process, name, type=None, required=True):
        super(OutputPort, self).__init__(process, name, type)
        self._required = required

    @property
    def required(self):
        return self._required
