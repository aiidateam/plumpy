from abc import ABCMeta, abstractmethod
from uuid import uuid1

from plum import exceptions
from plum.exceptions import CancelledError
from . import futures

__all__ = ['LoopObject', 'Ticking', 'Awaitable']


class LoopObject(object):
    def __init__(self, loop, uuid=None):
        if uuid is None:
            self._uuid = uuid1()
        else:
            self._uuid = uuid

        self._loop = loop

    @property
    def uuid(self):
        return self._uuid

    def on_loop_inserted(self, loop):
        """
        Called when the object is inserted into the event loop.

        :param loop: The event loop
        :type loop: `plum.event_loop.AbstractEventLoop`
        """
        pass

    def on_loop_removed(self):
        """
        Called when the object is removed from the event loop.
        """
        if self._loop is None:
            raise RuntimeError("Not in an event loop")

        self._loop = None

    def loop(self):
        """
        Get the event loop, can be None.
        :return: The event loop
        :rtype: :class:`plum.loop.event_loop.AbstractEventLoop`
        """
        return self._loop

    def in_loop(self):
        try:
            self.loop().get_object(self.uuid)
            return True
        except ValueError:
            return False


class Ticking(object):
    __metaclass__ = ABCMeta

    def on_loop_inserted(self, loop):
        super(Ticking, self).on_loop_inserted(loop)
        self._callback_handle = loop.call_soon(self._tick)

    @abstractmethod
    def tick(self):
        pass

    def pause(self):
        self._callback_handle.cancel()
        self._callback_handle = None

    def play(self):
        if self._callback_handle is None:
            self._callback_handle = self.loop().call_soon(self._tick)

    def _tick(self):
        self.tick()
        if self._callback_handle is not None:
            self._callback_handle = self.loop().call_soon(self._tick)


class Awaitable(object):
    def __init__(self, loop):
        """
        :param loop: :class:`plum.loop.event_loop.AbstractEventLoop` 
        """
        super(Awaitable, self).__init__(loop)
        self._future = loop.create_future()
        self._future.add_done_callback(self._future_done)

    def future(self):
        return self._future

    def done(self):
        return self.future().done()

    def result(self):
        return self.future().result()

    def set_result(self, result):
        self.future().set_result(result)
        try:
            self.loop().remove(self)
        except ValueError:
            pass

    def set_exception(self, exception):
        self.future().set_exception(exception)
        try:
            self.loop().remove(self)
        except ValueError:
            pass

    def cancel(self):
        return self._future.cancel()

    def cancelled(self):
        return self._future.cancelled()

    def _future_done(self, future):
        if future.cancelled():
            try:
                self.loop().remove(self)
            except ValueError:
                pass


class _GatheringFuture(futures.Future):
    def __init__(self, children, loop):
        super(_GatheringFuture, self).__init__(loop)
        self._children = children
        self._n_done = 0

        for child in self._children:
            child.add_done_callback(self._child_done)

    def cancel(self):
        if self.done():
            return False

        ret = False
        for child in self._children:
            if child.cancel():
                ret = True

        return ret

    def _child_done(self, future):
        if self.done():
            return

        try:
            if future.exception() is not None:
                self.set_exception(future.exception())
                return
        except CancelledError:
            self.set_exception(CancelledError())
            return

        self._n_done += 1
        if self._n_done == len(self._children):
            self._all_done()

    def _all_done(self):
        self.set_result([child.result() for child in self._children])


def gather(tasks_or_futures, loop):
    if isinstance(tasks_or_futures, futures.Future):
        return tasks_or_futures

    futs = [futures.get_future(task_or_future) for task_or_future in tasks_or_futures]
    return _GatheringFuture(futs, loop)
