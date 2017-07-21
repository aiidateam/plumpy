import thread
import time
from abc import ABCMeta, abstractmethod
from collections import deque
import logging

from . import futures
from . import events
from plum.loop.direct_executor import DirectExecutor as _DirectExecutor
from plum.loop.messages import Mailman
from plum.loop.tasks import Task
from plum.util import EventHelper

_LOGGER = logging.getLogger(__name__)

__all__ = ['LoopListener', 'BaseEventLoop']


class LoopListener(object):
    def on_object_inserted(self, loop, loop_object):
        pass

    def on_object_removed(self, loop, loop_object):
        pass


class AbstractEventLoop(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def create_future(self):
        """

        :return: A new future
        :rtype: :class:`futures.Future`
        """
        pass

    @abstractmethod
    def run_forever(self):
        pass

    @abstractmethod
    def run_until_complete(self, future):
        pass

    @abstractmethod
    def call_soon(self, fn, *args):
        pass

    @abstractmethod
    def call_later(self, delay, callback, *args):
        """
        Schedule `callback` to be called after the given `delay` in seconds.
         
        :param delay: The callback delay
        :type delay: float
        :param callback: The callback to call
        :param args: The callback arguments
        :return: A callback handle
        :rtype: :class:`events.Handle`
        """
        pass

    @abstractmethod
    def insert(self, loop_object):
        pass

    @abstractmethod
    def remove(self, loop_object):
        pass

    @abstractmethod
    def add_loop_listener(self, listener):
        pass

    @abstractmethod
    def remove_loop_listener(self, listener):
        pass

    @abstractmethod
    def time(self):
        pass

    @abstractmethod
    def messages(self):
        pass

    @abstractmethod
    def get_object(self, uuid):
        pass

    @abstractmethod
    def objects(self, obj_type=None):
        """
        Get the objects in the event loop.  Optionally filer for loop objects of
        a given type.
        
        :param obj_type: The loop object class to filter for. 
        :return: A list of the found objects.
        """
        pass

    # region Tasks
    @abstractmethod
    def create(self, task, *args, **kwargs):
        """
        Create a task.
        
        :param task: The task identifier 
        :param args: (optional) positional arguments to the task
        :param kwargs: (optional) keyword arguments to the task
        
        :return: The task object
        """
        pass

    @abstractmethod
    def set_object_factory(self, factory):
        """
        Set the factory used by :class:`AbstractEventLoop.create_task()`.
        
        If `None` then the default will be set.
        
        The factory should be a callabke with signature matching `(loop, task, *args, **kwargs)`
        where task is some task identifier and positional and keyword arguments
        can be supplied and it returns the :class:`Task` instance.
        
        :param factory: The task factory 
        """
        pass

    @abstractmethod
    def get_object_factory(self):
        """
        Get the task factory currently in use.  Returns `None` if the default is
        being used.
        
        :return: The task factory
        """
        pass

        # endregion

    @abstractmethod
    def close(self):
        """ Shutdown the event loop"""
        pass


class BaseEventLoop(AbstractEventLoop):
    def __init__(self):
        self._stopping = False
        self._callbacks = deque()

        self._objects = {}
        self._object_factory = None

        self._to_insert = []
        self._to_remove = set()
        self._thread_id = None

        self.__mailman = Mailman(self)
        self.__event_helper = EventHelper(LoopListener)

    def is_running(self):
        """
        Returns True if the event loop is running.
        
        :return: True if running, False otherwise
        :rtype: bool
        """
        return self._thread_id is not None

    def create_future(self):
        return futures.Future(self)

    def run_forever(self):
        self._thread_id = thread.get_ident()

        try:
            while not self._stopping:
                self._tick()

        finally:
            self._stopping = False
            self._thread_id = None

    def run_until_complete(self, future):
        """
        :param future: The future
        :type future: :class:`futures.Future`
        :return: The result of the future
        """
        future = futures.get_future(future)

        future.add_done_callback(self._run_until_complete_cb)
        self.run_forever()

        return future.result()

    def call_soon(self, fn, *args):
        """
        Call a callback function on the next tick

        :param fn: The callback function
        :param args: The function arguments
        :return: A callback handle
        :rtype: :class:`plum.loop.events.Handle`
        """
        handle = events.Handle(fn, args, self)
        self._callbacks.append(handle)
        return handle

    def call_later(self, delay, callback, *args):
        timer = self.create(events.Timer, self.time() + delay, callback, args)
        self.insert(timer)
        return timer

    def insert(self, loop_object):
        if loop_object.uuid in self._objects:
            return

        self._do_insert(loop_object)

    def _do_insert(self, loop_object):
        if self.is_running():
            self._to_insert.append(loop_object)
        else:
            self._insert(loop_object)

    def remove(self, loop_object):
        """
        Remove an object from the event loop.  If the event loop is not running
        the object is removed immediately, otherwise it is scheduled to be
        removed at the end of the next tick.
        
        :param loop_object: The object to remove 
        :return: True if it was removed, False if it was scheduled to be removed
        :rtype: bool
        """
        if loop_object.uuid not in self._objects:
            raise ValueError("Unknown loop object")
        if loop_object.uuid in self._to_remove:
            raise ValueError("Loop object already scheduled to be removed")

        if self.is_running():
            self._to_remove.add(loop_object.uuid)
            return False
        else:
            self._remove(loop_object.uuid)
            return True

    def objects(self, obj_type=None):
        # Filter the type if necessary
        if obj_type is not None:
            return [obj for obj in self._objects.itervalues() if isinstance(obj, obj_type)]
        else:
            return self._objects.values()

    def get_object(self, uuid):
        try:
            return self._objects[uuid]
        except KeyError:
            pass

        for obj in self._to_insert:
            if obj.uuid == uuid:
                return obj

        raise ValueError("Unknown uuid")

    def stop(self):
        """
        Stop the running event loop. 
        """
        self._stopping = True

    def tick(self):
        self._thread_id = thread.get_ident()
        try:
            self._tick()
        finally:
            self._thread_id = None

    def add_loop_listener(self, listener):
        self.__event_helper.add_listener(listener)

    def remove_loop_listener(self, listener):
        self.__event_helper.remove_listener(listener)

    def time(self):
        return time.time()

    def messages(self):
        return self.__mailman

    # region Tasks
    def create(self, object_type, *args, **kwargs):
        if self._object_factory is None:
            object_type = object_type(self, *args, **kwargs)
        else:
            object_type = self._object_factory(self, object_type, *args, **kwargs)

        self.insert(object_type)
        return object_type

    def set_object_factory(self, factory):
        self._object_factory = factory

    def get_object_factory(self):
        return self._object_factory

    # endregion

    def close(self):
        assert not self.is_running(), "Can't close a running loop"

        self._stopping = False
        self._callbacks = None

        self._objects = None
        self._object_factory = None

        self._to_insert = None
        self._to_remove = None
        self._thread_id = None

        self.__mailman = None
        self.__event_helper = None

    def _tick(self):
        # Insert any new processes
        for loop_object in self._to_insert:
            self._insert(loop_object)
        del self._to_insert[:]

        # Call all the callbacks
        todo = len(self._callbacks)
        for i in range(todo):
            handle = self._callbacks.popleft()
            if handle._cancelled:
                continue

            handle._run()

        # Finally deal with processes to be removed
        for pid in self._to_remove:
            self._remove(pid)
        self._to_remove.clear()

    def _run_until_complete_cb(self, fut):
        self.stop()

    def _insert(self, loop_object):
        self._objects[loop_object.uuid] = loop_object
        loop_object.on_loop_inserted(self)

        self.messages().send("loop.object.{}.inserted".format(loop_object.uuid))
        self.call_soon(self.__event_helper.fire_event, LoopListener.on_object_inserted, self, loop_object)

    def _remove(self, uuid):
        try:
            loop_object = self._objects.pop(uuid)
        except KeyError:
            raise ValueError("Unknown uuid")
        else:
            try:
                loop_object.on_loop_removed()
            except BaseException as e:
                _LOGGER.warning(
                    "Object '{}' produced exception '{}' during on_loop_removed".format(loop_object, e)
                )

            self.messages().send("loop.object.{}.removed".format(uuid))
            self.call_soon(self.__event_helper.fire_event, LoopListener.on_object_removed, self, loop_object)
