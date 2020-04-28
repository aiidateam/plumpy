import abc
import importlib

__all__ = ['ObjectLoader', 'DefaultObjectLoader', 'set_object_loader', 'get_object_loader']


class ObjectLoader(metaclass=abc.ABCMeta):
    """
    An abstract object loaders. Concrete implementations can be used to identify an
    object and load it with that identifier.
    """

    @abc.abstractmethod
    def load_object(self, identifier):
        """
        Given an identifier load an object.

        Throws a ValueError if the object cannot be loaded.

        :param identifier: The identifier
        :return: The loaded object
        """
        pass

    @abc.abstractmethod
    def identify_object(self, obj):
        """
        Get an identifier for an object.

        Throws a ValueError if the object cannot be identified.

        :param obj: The object to identify
        :return: An identifier for the object
        """
        pass


class DefaultObjectLoader(ObjectLoader):
    """
    A default implementation for an object loader.  Can load module level
    classes, functions and constants.
    """

    def load_object(self, identifier):
        mod, name = identifier.split(":")
        try:
            mod = importlib.import_module(mod)
        except ImportError as e:
            raise ValueError("module '{}' from identifier '{}' could not be loaded".format(mod, identifier))
        else:
            try:
                return getattr(mod, name)
            except AttributeError:
                raise ValueError("object '{}' form identifier '{}' could not be loaded".format(name, identifier))

    def identify_object(self, obj):
        identifier = "{}:{}".format(obj.__module__, obj.__name__)
        # Make sure we can load the object
        self.load_object(identifier)
        return identifier


_object_loader = None


def get_object_loader():
    """
    Get the plumpy global class loader

    :return: A class loader
    :rtype: :class:`ObjectLoader`
    """
    global _object_loader
    if _object_loader is None:
        _object_loader = DefaultObjectLoader()
    return _object_loader


def set_object_loader(loader):
    """
    Set the plumpy global object loader

    :param loader: An object loader
    :type loader: :class:`ObjectLoader`
    :return:
    """
    global _object_loader
    _object_loader = loader
