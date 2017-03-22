import collections
import copy
import functools
import operator
from plum.util import override
from plum.class_loader import ClassLoader


# For now a bundle is just a dictionary
class Bundle(collections.MutableMapping):
    # Some common keys
    CLASS = 'class'

    def __init__(self, *args, **kwargs):
        self.__dict = dict(*args, **kwargs)
        self.__hash = None
        self._class_loader = ClassLoader()

    def set_class_loader(self, loader):
        self._class_loader = loader

    def get_class_loader(self):
        return self._class_loader

    def get_dict(self):
        return self.__dict

    def get_dict_deepcopy(self):
        return copy.deepcopy(self.__dict)

    def set_if_not_none(self, key, value):
        """
        Set a key to a value in this bundle if the value is not None, otherwise
        do nothing.

        :param key: The key
        :type key: str
        :param value: The value to set
        """
        if value is not None:
            self[key] = value

    # From MutableMapping
    @override
    def __getitem__(self, key):
        return self.__dict[key]

    @override
    def __setitem__(self, key, value):
        self.__dict[key] = value

    @override
    def __delitem__(self, key):
        del self.__dict[key]

    def copy(self, **add_or_replace):
        b = Bundle(self._class_loader)
        b.__dict.update(self.__dict)
        return b

    @override
    def __iter__(self):
        return iter(self.__dict)

    @override
    def __len__(self):
        return len(self.__dict)

    @override
    def __repr__(self):
        return 'Bundle(%r)' % self.get_dict()

    @override
    def __hash__(self):
        if self.__hash is None:
            hashes = map(hash, self.items())
            self.__hash = functools.reduce(operator.xor, hashes, 0)

        return self.__hash
