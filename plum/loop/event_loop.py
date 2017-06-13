from abc import ABCMeta, abstractmethod
from collections import deque, namedtuple
import thread
import uuid as libuuid
import plum.process

Terminated = namedtuple("Terminated", ['result'])


class InvalidStateError(Exception):
    pass


class Handle(object):
    def __init__(self, fn, args, loop):
        self._loop = loop
        self._fn = fn
        self._args = args
        self._cancelled = False

    def cancel(self):
        if not self._cancelled:
            self._cancelled = True
            self._fn = None
            self._args = None

    def _run(self):
        self._fn(*self._args)


_PENDING = 'PENDING'
_CANCELLED = 'CANCELLED'
_FINISHED = 'FINISHED'


class Future(object):
    _UNSET = ()

    def __init__(self, loop, uuid=None):
        self._loop = loop
        if uuid is None:
            self._uuid = libuuid.uuid4()
        else:
            self._uuid = uuid
        self._state = _PENDING
        self._result = self._UNSET
        self._exception = None
        self._callbacks = []

    def uuid(self):
        return self._uuid

    def done(self):
        return self._state != _PENDING

    def result(self):
        if self._state is not _FINISHED:
            raise InvalidStateError("The future has not completed yet")
        elif self._exception is not None:
            raise self._exception

        return self._result

    def set_result(self, result):
        if self.done():
            raise InvalidStateError("The future is already done")

        self._result = result
        self._state = _FINISHED
        self._schedule_callbacks()

    def exception(self):
        if self._state is not _FINISHED:
            raise InvalidStateError("Exception not set")

        return self._exception

    def set_exception(self, exception):
        if self.done():
            raise InvalidStateError("The future is already done")

        self._exception = exception
        self._state = _FINISHED
        self._schedule_callbacks()

    def add_done_callback(self, fn):
        """
        Add a callback to be run when the future becomes done.
        
        :param fn: The callback function.
        """
        if self._state != _PENDING:
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
    def __init__(self):
        self._stopping = False
        self._callbacks = deque()

        self._objects = {}
        self._processes = {}
        self._to_insert = []
        self._to_remove = set()
        self._thread_id = None

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
        from plum.process import Process

        if isinstance(future, Process):
            future = self.insert(future)

        future.add_done_callback(self._run_until_complete_cb)
        self.run_forever()

        return future.result()

    def call_soon(self, fn, *args):
        handle = Handle(fn, args, self)
        self._callbacks.append(handle)
        return handle

    def insert(self, loop_object):
        if loop_object.uuid in self._objects:
            return self._objects[loop_object.uuid].future()

        for handle in self._to_insert:
            if loop_object.uuid == handle.object().uuid:
                return handle.future()

        future = self.create_future()
        handle = _ObjectHandle(loop_object, future, self)
        self._to_insert.append(handle)

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

    def objects(self):
        return [h.object() for h in self._objects.itervalues()]

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
        return [self._objects[uuid].object() for uuid in self._processes.itervalues()]

    def get_process(self, pid):
        try:
            self._objects[self._processes[pid]].future()
        except KeyError:
            pass

        for handle in self._to_insert:
            if isinstance(handle.object(), plum.process.Process) and \
                    handle.object().pid == pid:
                return handle.object()

        raise ValueError("Unknown pid")

    def get_process_future(self, pid):
        try:
            self._objects[self._processes[pid]].future()
        except KeyError:
            pass

        for handle in self._to_insert:
            if isinstance(handle.object(), plum.process.Process) and \
                    handle.object().pid == pid:
                return handle.future()

        raise ValueError("Unknown pid")

    def stop(self):
        """
        Stop the running event loop. 
        """
        self._stopping = True

    def tick(self):
        self._tick()

    def _tick(self):
        # Insert any new processes
        for handle in self._to_insert:
            object = handle.object()
            self._objects[object.uuid] = handle
            if isinstance(object, plum.process.Process):
                self._processes[object.pid] = object.uuid

            object.on_loop_inserted(self)
        del self._to_insert[:]

        # Tick the processes
        for handle in self._objects.values():
            handle.tick()

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

    def _remove_process(self, uuid):
        try:
            handle = self._objects.pop(uuid)
        except KeyError:
            raise ValueError("Unknown uuid")
        else:
            if isinstance(handle.object(), plum.process.Process):
                self._processes.pop(handle.object().pid)
            handle.object().on_loop_removed()
