
import importlib
from plum.exceptions import ClassNotFoundException
from . import utils


class ClassLoader(object):
    def __init__(self, parent=None):
        self._parent = parent

    @staticmethod
    def find_class(name):
        """
        Load a class from a string
        """
        return utils.load_object(name)

    def load_class(self, name):
        # Try the parent first
        if self._parent is not None:
            Class = self._parent.find_class(name)
            if Class is not None:
                return Class

        return self.find_class(name)
