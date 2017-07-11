from abc import ABCMeta, abstractmethod
from collections import namedtuple
from uuid import uuid1
from . import futures

__all__ = ['LoopObject', 'Ticking', 'Task']


class LoopObject(object):
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
        Called when the object is removed from the event loop.
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


class Ticking(object):
    __metaclass__ = ABCMeta

    def on_loop_inserted(self, loop):
        super(Ticking, self).on_loop_inserted(loop)
        loop.start_ticking(self)

    def on_loop_removed(self):
        self.loop().stop_ticking(self)
        super(Ticking, self).on_loop_removed()

    @abstractmethod
    def tick(self):
        pass


class Await(object):
    def __init__(self, callback, *args):
        self.callback = callback
        self.futures = args


class Continue(object):
    def __init__(self, callback):
        self.callback = callback


class Task(Ticking, LoopObject):
    __metaclass__ = ABCMeta

    Terminated = namedtuple("Terminated", ['result'])

    def __init__(self, loop):
        super(Task, self).__init__()
        self._wait_on_future = None
        self._future = loop.create_future()
        self._future.add_done_callback(self._future_done)

    def future(self):
        return self._future

    def tick(self):
        try:
            result = self.execute()
        except BaseException as e:
            self.loop().remove(self)
            self.future().set_exception(e)
        else:
            if isinstance(result, futures.Future):
                self._wait_on_future = result
                self._wait_on_future.add_done_callback(self._wait_on_future_cb)
                self.loop().stop_ticking(self)

            if isinstance(result, self.Terminated):
                self.future().set_result(result.result)
                self.loop().remove(self)

    def on_loop_removed(self):
        super(Task, self).on_loop_removed()
        self._future = None
        if self._wait_on_future is not None:
            self._wait_on_future.remove_done_callback(self._wait_on_future_cb)
            self._wait_on_future = None

    def play(self):
        """
        Play the task, i.e. start ticking if in the loop and not currently ticking.
        
        It is an error to call play if not inserted in an event loop or if currently
        playing.
        """
        self.loop().start_ticking(self)

    def pause(self):
        """
        Pause a ticking task.
        """
        self.loop().stop_ticking(self)

    def is_playing(self):
        return self.loop().is_ticking(self)

    @abstractmethod
    def execute(self):
        pass

    def _wait_on_future_cb(self, future):
        self.loop().start_ticking(self)
        self._wait_on_future = None

    def _future_done(self, future):
        if future.cancelled():
            self.loop().remove(self)
