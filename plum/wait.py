# -*- coding: utf-8 -*-

import apricotpy.persistable as apricotpy


class WaitOn(apricotpy.AwaitableLoopObject):
    """
    An object that represents something that is being waited on.
    """

    def __str__(self):
        return self.__class__.__name__
