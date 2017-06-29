import itertools
import thread
import time
from abc import ABCMeta, abstractmethod
from collections import deque, namedtuple

import plum.process
import plum.util
from plum.loop import events
from plum.loop.direct_executor import DirectExecutor as _DirectExecutor
from plum.loop.messages import Mailman
from plum.loop.object import LoopObject
from plum.util import EventHelper

Terminated = namedtuple("Terminated", ['result'])

_PENDING = 'PENDING'
_CANCELLED = 'CANCELLED'
_FINISHED = 'FINISHED'


class Future(plum.util.Future):
    def __init__(self, loop):
        super(Future, self).__init__()
        self._loop = loop
        self._callbacks = []

    def loop(self):
        return self._loop

    def set_result(self, result):
        super(Future, self).set_result(result)
        self._schedule_callbacks()

    def set_exception(self, exception):
        super(Future, self).set_exception(exception)
        self._schedule_callbacks()

    def add_done_callback(self, fn):
        """
        Add a callback to be run when the future becomes done.
        
        :param fn: The callback function.
        """
        if self.done():
            self._loop.call_soon(fn, self)
        else:
            self._callbacks.append(fn)

    def _schedule_callbacks(self):
        """
        Ask the event loop to call all callbacks.
        
        The callbacks are scheduled to be called as soon as possible.
        """
        callbacks = self._callbacks[:]
        if not callbacks:
            return

        self._callbacks[:] = []
        for callback in callbacks:
            self._loop.call_soon(callback, self)


class LoopListener(object):
    def on_object_inserted(self, loop, loop_object):
        pass

    def on_object_removed(self, loop, loop_object):
        pass


class AbstractEventLoop(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def create_future(self):
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
        pass

    @abstractmethod
    def insert(self, loop_object):
        pass

    @abstractmethod
    def remove(self, loop_object):
        pass

    @abstractmethod
    def get_process(self, pid):
        pass

    @abstractmethod
    def get_process_future(self, pid):
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


class _ObjectHandle(object):
    def __init__(self, loop_object, future, loop):
        self._loop = loop
        self._loop_object = loop_object
        self._future = future

    def object(self):
        return self._loop_object

    def future(self):
        return self._future

    def tick(self):
        try:
            result = self._loop_object.tick()
        except BaseException as e:
            self._future.set_exception(e)
            self._loop.remove(self._loop_object)
        else:
            if isinstance(result, Terminated):
                self._loop.remove(self._loop_object)
                self._future.set_result(result.result)


class BaseEventLoop(AbstractEventLoop):
    def __init__(self, executor=None):
        if executor is None:
            self._executor = _DirectExecutor()
        else:
            self._executor = executor

        self._stopping = False
        self._callbacks = deque()

        self._objects = {}
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
        return Future(self)

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
        :param future: The future or a process
        :type future: union(:class:`Future`, :class:`Process`)
        :return: The result of the future
        """
        if isinstance(future, LoopObject):
            future = self.insert(future)

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
        timer = events.Timer(self.time() + delay, callback, args)
        self.insert(timer)
        return timer

    def insert(self, loop_object):
        if loop_object.uuid in self._objects:
            return self._objects[loop_object.uuid].future()

        for handle in self._to_insert:
            if loop_object is handle.object():
                return handle.future()

        future = self.create_future()
        handle = _ObjectHandle(loop_object, future, self)

        if self.is_running():
            self._to_insert.append(handle)
        else:
            self._insert_process(handle)

        return future

    def remove(self, loop_object):
        """
        Remove a process from the event loop.  If the event loop is not running
        the process is removed immediately, otherwise it is scheduled to be
        removed at the end of the next tick.
        
        :param loop_object: The process to remove 
        :return: True if it was removed, False if it was scheduled to be removed
        :rtype: bool
        """
        if self.is_running():
            self._to_remove.add(loop_object.uuid)
            return False
        else:
            self._remove_process(loop_object.uuid)
            return True

    def objects(self, type=None):
        objs = [h.object() for h in self._objects.itervalues()]

        # Filter the type if necessary
        if type is not None:
            return [obj for obj in objs if isinstance(obj, type)]
        else:
            return objs

    def get_object(self, uuid):
        try:
            self._objects[uuid].object()
        except KeyError:
            pass

        for handle in self._to_insert:
            if handle.object().uuid == uuid:
                return handle.object()

        raise ValueError("Unknown uuid")

    def get_object_future(self, uuid):
        try:
            self._objects[uuid].future()
        except KeyError:
            pass

        for handle in self._to_insert:
            if handle.object().uuid == uuid:
                return handle.future()

        raise ValueError("Unknown uuid")

    def processes(self):
        return self.objects(type=plum.Process)

    def get_process(self, pid):
        for handle in itertools.chain(self._objects.itervalues(), self._to_insert):
            if isinstance(handle.object(), plum.process.Process) and \
                            handle.object().pid == pid:
                return handle.object()

        raise ValueError("Unknown pid")

    def get_process_future(self, pid):
        for handle in itertools.chain(self._objects.itervalues(), self._to_insert):
            if isinstance(handle.object(), plum.process.Process) and \
                            handle.object().pid == pid:
                return handle.future()

        raise ValueError("Unknown pid '{}'".format(pid))

    def stop(self):
        """
        Stop the running event loop. 
        """
        self._stopping = True

    def tick(self):
        self._tick()

    def add_loop_listener(self, listener):
        self.__event_helper.add_listener(listener)

    def remove_loop_listener(self, listener):
        self.__event_helper.remove_listener(listener)

    def time(self):
        return time.time()

    def messages(self):
        return self.__mailman

    def _tick(self):
        # Insert any new processes
        for handle in self._to_insert:
            self._insert_process(handle)
        del self._to_insert[:]

        # Tick the processes
        futs = []
        for handle in self._objects.values():
            futs.append(self._executor.submit(handle.tick))

        # Wait for everything to complete
        for fut in futs:
            fut.result()

        # Call all the callbacks
        todo = len(self._callbacks)
        for i in range(todo):
            handle = self._callbacks.popleft()
            if handle._cancelled:
                continue

            handle._run()

        # Finally deal with processes to be removed
        for pid in self._to_remove:
            self._remove_process(pid)
        self._to_remove.clear()

    def _run_until_complete_cb(self, fut):
        self.stop()

    def _insert_process(self, handle):
        obj = handle.object()
        self._objects[obj.uuid] = handle
        obj.on_loop_inserted(self)

        self.call_soon(self.__event_helper.fire_event, LoopListener.on_object_inserted, self, obj)

    def _remove_process(self, uuid):
        try:
            handle = self._objects.pop(uuid)
        except KeyError:
            raise ValueError("Unknown uuid")
        else:
            handle.object().on_loop_removed()
            self.call_soon(self.__event_helper.fire_event, LoopListener.on_object_removed, self, handle.object())
