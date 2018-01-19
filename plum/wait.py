# -*- coding: utf-8 -*-

class WaitOn(object):
    """
    An object that represents something that is being waited on.
    """

    def __str__(self):
        return self.__class__.__name__