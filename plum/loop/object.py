from abc import ABCMeta, abstractmethod
from uuid import uuid1


class LoopObject(object):
    __metaclass__ = ABCMeta

    def __init__(self, uuid=None):
        if uuid is None:
            self.__uuid = uuid1()
        else:
            self.__uuid = uuid

        self.__loop = None

    @property
    def uuid(self):
        return self.__uuid

    def on_loop_inserted(self, loop):
        """
        Called when the object is inserted into the event loop.

        :param loop: The event loop
        :type loop: `plum.event_loop.AbstractEventLoop`
        """
        if self.__loop is not None:
            raise RuntimeError("Already in an event loop")

        self.__loop = loop

    def on_loop_removed(self):
        """
        Called when the process is removed from the event loop.
        """
        if self.__loop is None:
            raise RuntimeError("Not in an event loop")

        self.__loop = None

    def loop(self):
        """
        Get the event loop, can be None.
        :return: The event loop
        :rtype: :class:`plum.loop.event_loop.AbstractEventLoop`
        """
        return self.__loop

    @abstractmethod
    def tick(self):
        pass
