from . import utils


class ClassLoader(object):
    def __init__(self, parent=None):
        self._parent = parent

    @classmethod
    def find_class(cls, name):
        """
        Load a class from a string
        """
        return utils.load_object(name)

    def load_class(self, name):
        # Try the parent first
        if self._parent is not None:
            obj_class = self._parent.find_class(name)
            if obj_class is not None:
                return obj_class

        return self.find_class(name)
