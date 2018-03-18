from . import utils

__all__ = ['ClassLoader', 'set_class_loader', 'get_class_loader']


class ClassLoader(object):
    """
    An object to load classes based on an identifier.
    """

    def __init__(self, parent=None):
        self._parent = parent

    @classmethod
    def find_class(cls, identifier):
        """
        Load a class from a string
        """
        return utils.load_object(identifier)

    def load_class(self, identifier):
        """
        Get a class based on its identifier.

        :param identifier: The class identifier
        :return: The class object
        :rtype: type
        """
        # Try the parent first
        if self._parent is not None:
            obj_class = self._parent.find_class(identifier)
            if obj_class is not None:
                return obj_class

        return self.find_class(identifier)

    def class_identifier(self, obj):
        """
        Get a class identifier for an object.  The object can either be a class
        or a class instance.  The identifier can subsequently be used to get the
        class using `load_class()`.

        :param obj: The object to get the identifier for
        :type obj: type or object
        """
        return utils.class_name(obj, self)


class InMemoryClassLoader(ClassLoader):
    def __init__(self):
        super(InMemoryClassLoader, self).__init__()
        self._classes = {}

    def register(self, cls):
        self._classes[id(cls)] = cls

    def find_class(self, identifier):
        try:
            return self._classes[identifier]
        except KeyError:
            raise ValueError("Unknown class '{}'".format(identifier))

    def class_identifier(self, obj):
        for identifier, cls in self._classes.items():
            if obj is cls:
                return identifier

        raise ValueError("Unknown class")


_class_loader = None


def get_class_loader():
    """
    Get the plumpy global class loader

    :return: A class loader
    :rtype: :class:`ClassLoader`
    """
    global _class_loader
    if _class_loader is None:
        _class_loader = ClassLoader()
    return _class_loader


def set_class_loader(class_loader):
    """
    Set the plumpy global class loader

    :param class_loader: A class loader
    :type class_loader: :class:`ClassLoader`
    :return:
    """
    global _class_loader
    _class_loader = class_loader
