
import importlib
from plum.exceptions import ClassNotFoundException
from plum.util import load_class


class ClassLoader(object):
    def __init__(self, parent=None):
        self._parent = parent

    def find_class(self, name):
        """
        Load a class from a string
        """
        class_data = name.split(".")
        module_path = ".".join(class_data[:-1])
        class_name = class_data[-1]

        module = importlib.import_module(module_path)

        # Finally, retrieve the class
        try:
            return getattr(module, class_name)
        except AttributeError:
            raise ClassNotFoundException("Class {} not found".format(name))

    def load_class(self, name):
        # Try the parent first
        if self._parent is not None:
            Class = self._parent.find_class(name)
            if Class is not None:
                return Class

        return self.find_class(name)