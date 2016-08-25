
import collections
import functools
import operator
from plum.util import override
from plum.class_loader import ClassLoader


# For now a bundle is just a dictionary
class Bundle(collections.MutableMapping):
    def __init__(self, *args, ** kwargs):
        self.__dict = dict(*args, **kwargs)
        self.__hash = None
        self._class_loader = ClassLoader()

    def set_class_loader(self, loader):
        self._class_loader = loader

    def get_class_loader(self):
        return self._class_loader

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

    @override
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
        return '<Bundle %s>' % repr(self.__dict)

    @override
    def __hash__(self):
        if self.__hash is None:
            hashes = map(hash, self.items())
            self.__hash = functools.reduce(operator.xor, hashes, 0)

        return self.__hash
    ##########################
