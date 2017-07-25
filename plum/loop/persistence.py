import logging
from abc import ABCMeta, abstractmethod

import plum.loop
from plum import util, exceptions
from plum.loop.tasks import Task
from . import objects
from plum.persistence import Bundle
from plum.loop.objects import Awaitable

_LOGGER = logging.getLogger(__name__)


class Persistable(object):
    """
    An abstract class persistable objects.
    """
    __metaclass__ = ABCMeta

    CLASS_NAME = 'class_name'

    @classmethod
    def create_from(cls, loop, saved_state, *args):
        """
        Create an object from a saved instance state.

        :param loop: The event loop
        :type loop: :class:`plum.loop.event_loop.AbstractEventLoop`
        :param saved_state: The saved state
        :type saved_state: :class:`plum.persistence.Bundle`
        :return: An instance of this task with its state loaded from the save state.
        """
        # Get the class using the class loader and instantiate it
        class_name = saved_state[cls.CLASS_NAME]
        my_name = util.fullname(cls)
        if class_name != my_name:
            _LOGGER.warning(
                "Loading class from a bundle that was created from a class with a different "
                "name.  This class is '{}', bundle created by '{}'".format(class_name, my_name))

        task = cls.__new__(cls)
        task.load_instance_state(loop, saved_state, *args)
        return task

    def save_instance_state(self, out_state):
        out_state[self.CLASS_NAME] = util.fullname(self)

    @abstractmethod
    def load_instance_state(self, loop, saved_state, *args):
        pass


def load_from(loop, saved_state, *args):
    # Get the class using the class loader and instantiate it
    class_name = saved_state[Persistable.CLASS_NAME]
    task_class = saved_state.get_class_loader().load_class(class_name)
    return loop.create(task_class, saved_state, *args)


class PersistableLoopObjectMixin(Persistable):
    UUID = 'uuid'

    def __init__(self, loop):
        if not isinstance(self, objects.LoopObject):
            raise TypeError("Has to be used with a LoopObject")

        super(PersistableLoopObjectMixin, self).__init__(loop)

    def save_instance_state(self, out_state):
        super(PersistableLoopObjectMixin, self).save_instance_state(out_state)
        out_state[self.UUID] = self.uuid

    def load_instance_state(self, loop, saved_state, *args):
        super(PersistableLoopObjectMixin, self).load_instance_state(loop, saved_state, *args)
        self._loop = loop
        self._uuid = saved_state[self.UUID]


class PersistableLoopObject(PersistableLoopObjectMixin, objects.LoopObject):
    pass


class PersistableAwaitableMixin(Persistable):
    RESULT = 'result'
    EXCEPTION = 'exception'
    CANCELLED = 'cancelled'

    def __init__(self, loop):
        assert isinstance(self, Awaitable), "Has to be used with a Awaitable"
        super(PersistableAwaitableMixin, self).__init__(loop)

    def save_instance_state(self, out_state):
        super(PersistableAwaitableMixin, self).save_instance_state(out_state)

        if self.done():
            try:
                out_state[self.RESULT] = self.result()
            except exceptions.CancelledError:
                out_state[self.CANCELLED] = True
            except BaseException as e:
                out_state[self.EXCEPTION] = e

    def load_instance_state(self, loop, saved_state, *args):
        super(PersistableAwaitableMixin, self).load_instance_state(loop, saved_state, *args)
        self._future = loop.create_future()
        self._future.add_done_callback(self._future_done)

        try:
            self.set_result(saved_state[self.RESULT])
        except KeyError:
            try:
                self.set_exception(saved_state[self.EXCEPTION])
            except KeyError:
                try:
                    if saved_state[self.CANCELLED]:
                        self.cancel()
                except KeyError:
                    pass


class PersistableAwaitable(PersistableAwaitableMixin, Awaitable):
    pass


class ContextMixin(object):
    """
    Add a context to a Persistable.  The contents of the context will be saved
    in the instance state unlike standard instance variables.
    """
    CONTEXT = 'context'

    def __init__(self, loop):
        if not isinstance(self, Persistable):
            raise TypeError("Has to be used with a Persistable")

        super(ContextMixin, self).__init__(loop)
        self._context = util.SimpleNamespace()

    @property
    def ctx(self):
        return self._context

    def save_instance_state(self, out_state):
        super(ContextMixin, self).save_instance_state(out_state)
        out_state[self.CONTEXT] = Bundle(self._context.__dict__)

    def load_instance_state(self, loop, saved_state, *args):
        super(ContextMixin, self).load_instance_state(loop, saved_state, *args)
        self._context = util.SimpleNamespace(**saved_state[self.CONTEXT].get_dict())


class PersistableTask(PersistableAwaitableMixin,
                      PersistableLoopObjectMixin,
                      Task):
    __metaclass__ = ABCMeta

    CLASS_NAME = 'class_name'
    AWAITING = 'awaiting'
    CALLBACK = 'callback'

    def load_instance_state(self, loop, saved_state, *args):
        super(PersistableTask, self).load_instance_state(loop, saved_state, *args)

        try:
            self._awaiting = load_from(self.loop(), saved_state[self.AWAITING])
        except KeyError:
            self._awaiting = None

        try:
            self._callback = getattr(self, saved_state[self.CALLBACK])
        except KeyError:
            self._callback = None

        # Runtime state
        self._paused = False
        self._tick_handle = None

    def save_instance_state(self, out_state):
        super(PersistableTask, self).save_instance_state(out_state)

        if self._awaiting is not None:
            try:
                bundle = Bundle()
                self._awaiting.save_instance_state(bundle)
            except AttributeError:
                raise RuntimeError("Awaitable is not persistable: '{}".format(awaiting.__class__))
            else:
                out_state[self.AWAITING] = bundle

        if self._callback is not None:
            out_state[self.CALLBACK] = self._callback.__name__
