import logging
from abc import ABCMeta, abstractmethod
from collections import namedtuple

from plum import util
from plum.loop import Ticking
from . import futures
from . import objects
from .objects import gather

__all__ = ['Continue', 'Await', 'Task']

_LOGGER = logging.getLogger(__name__)


class _TaskDirective(object):
    pass


class Continue(_TaskDirective):
    def __init__(self, callback):
        self.callback = callback


class Await(_TaskDirective):
    def __init__(self, callback, tasks_or_futures):
        self.callback = callback
        self.tasks_or_futures = tasks_or_futures


class Task(objects.Awaitable, objects.LoopObject):
    __metaclass__ = ABCMeta

    Terminated = namedtuple("Terminated", ['result'])

    def __init__(self, loop):
        super(Task, self).__init__(loop)

        self._awaiting = None
        self._callback = None
        self._tick_handle = None
        self._paused = False

    def awaiting(self):
        """
        :return: The awaitable this task is awaiting, or None
        :rtype: :class:`plum.loop.objects.Awaitable`
        """
        return self._awaiting

    def tick(self):
        self._tick_handle = None
        if self._paused:
            return

        try:
            if self._callback is not None:
                if self._awaiting is not None:
                    result = self._callback(self._awaiting.result())
                else:
                    result = self._callback()
                self._callback, self._awaiting = None, None
            else:
                # First time
                result = self.execute()
        except BaseException as e:
            self.set_exception(e)
        else:
            if isinstance(result, _TaskDirective):
                if isinstance(result, Continue):
                    self._callback = result.callback
                    self._tick_handle = self.loop().call_soon(self.tick)

                if isinstance(result, Await):
                    self._callback = result.callback
                    self._awaiting = result.tasks_or_futures
                    futures.get_future(self._awaiting).add_done_callback(self._await_done)
            else:
                self.set_result(result)

    def on_loop_inserted(self, loop):
        super(Task, self).on_loop_inserted(loop)
        if self._awaiting is None:
            loop.call_soon(self.tick)
        else:
            futures.get_future(self._awaiting).add_done_callback(self._await_done)

    def play(self):
        """
        Play the task if it was paused.
        """
        if self.done() or not self._paused:
            return False

        if self.awaiting() is not None:
            self._tick_handle = self.loop().call_soon(self.tick)

        self._paused = False

        return True

    def pause(self):
        """
        Pause a playing task.
        """
        if self._paused:
            return True

        if self.done():
            return False

        if self._tick_handle is not None:
            self._tick_handle.cancel()
        self._paused = True

        return True

    def is_playing(self):
        return not self._paused

    def cancel(self):
        cancelled = super(Task, self).cancel()
        if cancelled and self.awaiting() is not None:
            self.awaiting().cancel()
        return cancelled

    @abstractmethod
    def execute(self):
        pass

    def _await_done(self, future):
        self._awaiting = None
        if self.done():
            return

        if future.cancelled() and not self.cancelled():
            self.cancel()
        elif self._callback is not None:
            self._tick_handle = self.loop().call_soon(self.tick)
        else:
            self.set_result(future.result)
