
from plum.util import load_class


class ClassLoader(object):
    def __init__(self, parent=None):
        self._parent = parent

    def find_class(self, name):
        # Try the parent first
        if self._parent is not None:
            Class = self._parent.find_class(name)
            if Class is not None:
                return Class

        # Fallback to standard
        return load_class(name)