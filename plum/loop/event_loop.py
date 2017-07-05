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
from plum.loop.object import Task
from plum.util import EventHelper


class Future(plum.util.Future):
    def __init__(self, loop):
        super(Future, self).__init__()
        self._loop = loop
        self._callbacks = []

    def loop(self):
        return self._loop

    def cancel(self):
        if super(Future, self).cancel():
            self._schedule_callbacks()
            return True
        else:
            return False

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

    def remove_done_callback(self, fn):
        self._callbacks.remove(fn)

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

    @abstractmethod
    def start_ticking(self, loop_object):
        pass

    @abstractmethod
    def stop_ticking(self, loop_object):
        pass

    @abstractmethod
    def is_ticking(self, loop_object):
        """
        Is the event loop currently ticking the given loop object
        
        :param loop_object: The loop object 
        :return: True if ticking, False otherwise
        """
        pass


class BaseEventLoop(AbstractEventLoop):
    def __init__(self, executor=None):
        if executor is None:
            self._executor = _DirectExecutor()
        else:
            self._executor = executor

        self._stopping = False
        self._callbacks = deque()

        self._objects = {}
        self._ticking = []

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
        if isinstance(future, Task):
            future = self.insert_task(future)

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
            return

        if isinstance(loop_object, Task):
            self.insert_task(loop_object)
        else:
            self._do_insert(loop_object)

    def insert_task(self, task):
        if task.uuid in self._objects:
            return task.future()

        future = self.create_future()
        task.set_future(future)
        self._do_insert(task)

        return future

    def _do_insert(self, loop_object):
        if self.is_running():
            self._to_insert.append(loop_object)
        else:
            self._insert(loop_object)

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
            self._remove(loop_object.uuid)
            return True

    def objects(self, type=None):
        # Filter the type if necessary
        if type is not None:
            return [obj for obj in self._objects.itervalues() if isinstance(obj, type)]
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

    def processes(self):
        return self.objects(type=plum.Process)

    def get_process(self, pid):
        for obj in itertools.chain(self._objects.itervalues(), self._to_insert):
            if isinstance(obj, plum.process.Process) and obj.pid == pid:
                return obj

        raise ValueError("Unknown pid")

    def get_process_future(self, pid):
        return self.get_process(pid).future()

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

    def start_ticking(self, loop_object):
        if loop_object.loop() is not self:
            raise ValueError("Cannot tick an object not in the event loop")
        if loop_object not in self._ticking:
            self._ticking.append(loop_object)

    def stop_ticking(self, loop_object):
        if loop_object.loop() is not self:
            raise ValueError("Cannot stop ticking an object not in the event loop")
        if loop_object in self._ticking:
            self._ticking.remove(loop_object)

    def is_ticking(self, loop_object):
        return loop_object in self._ticking

    def _tick(self):
        # Insert any new processes
        for loop_object in self._to_insert:
            self._insert(loop_object)
        del self._to_insert[:]

        # Tick the processes, have to use a copy of the list as they may stop ticking
        # during the tick call
        futs = []
        for loop_object in list(self._ticking):
            futs.append(self._executor.submit(loop_object.tick))

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
            self._remove(pid)
        self._to_remove.clear()

    def _run_until_complete_cb(self, fut):
        self.stop()

    def _insert(self, loop_object):
        self._objects[loop_object.uuid] = loop_object
        loop_object.on_loop_inserted(self)

        self.call_soon(self.__event_helper.fire_event, LoopListener.on_object_inserted, self, loop_object)

    def _remove(self, uuid):
        try:
            loop_object = self._objects.pop(uuid)
        except KeyError:
            raise ValueError("Unknown uuid")
        else:
            loop_object.on_loop_removed()
            if loop_object in self._ticking:
                self._ticking.remove(loop_object)

            self.call_soon(self.__event_helper.fire_event, LoopListener.on_object_removed, self, loop_object)
