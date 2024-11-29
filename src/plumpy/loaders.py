# -*- coding: utf-8 -*-
import abc
import importlib
from typing import Any, Optional

__all__ = ['DefaultObjectLoader', 'ObjectLoader', 'get_object_loader', 'set_object_loader']


class ObjectLoader(metaclass=abc.ABCMeta):
    """
    An abstract object loaders. Concrete implementations can be used to identify an
    object and load it with that identifier.
    """

    @abc.abstractmethod
    def load_object(self, identifier: str) -> Any:
        """
        Given an identifier load an object.

        Throws a ValueError if the object cannot be loaded.

        :param identifier: The identifier
        :return: The loaded object
        """

    @abc.abstractmethod
    def identify_object(self, obj: Any) -> str:
        """
        Get an identifier for an object.

        Throws a ValueError if the object cannot be identified.

        :param obj: The object to identify
        :return: An identifier for the object
        """


class DefaultObjectLoader(ObjectLoader):
    """
    A default implementation for an object loader.  Can load module level
    classes, functions and constants.
    """

    def load_object(self, identifier: str) -> Any:
        try:
            mod_name, name = identifier.split(':')
        except ValueError as exc:
            raise ValueError(f'identifier `{identifier}` has an invalid format.') from exc

        try:
            mod = importlib.import_module(mod_name)
        except ImportError as exc:
            raise ValueError(f'module `{mod_name}` from identifier `{identifier}` could not be loaded.') from exc
        else:
            try:
                return getattr(mod, name)
            except AttributeError as exc:
                raise ValueError(f'object `{name}` form identifier `{identifier}` could not be loaded.') from exc

    def identify_object(self, obj: Any) -> str:
        identifier = f'{obj.__module__}:{obj.__name__}'
        # Make sure we can load the object
        self.load_object(identifier)
        return identifier


OBJECT_LOADER: Optional[ObjectLoader] = None


def get_object_loader() -> ObjectLoader:
    """
    Get the plumpy global class loader

    :return: A class loader
    :rtype: :class:`ObjectLoader`
    """
    global OBJECT_LOADER  # noqa: PLW0603
    if OBJECT_LOADER is None:
        OBJECT_LOADER = DefaultObjectLoader()
    return OBJECT_LOADER


def set_object_loader(loader: Optional[ObjectLoader]) -> None:
    """
    Set the plumpy global object loader

    :param loader: An object loader
    :type loader: :class:`ObjectLoader`
    :return:
    """
    global OBJECT_LOADER  # noqa: PLW0603
    OBJECT_LOADER = loader
