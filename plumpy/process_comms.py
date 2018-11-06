"""Module for process level communication functions and classes"""

from __future__ import absolute_import
from __future__ import print_function
import copy
import logging

from tornado import gen
import kiwipy

from . import loaders
from . import communications
from . import futures
from . import persistence
from . import exceptions

__all__ = [
    'PAUSE_MSG',
    'PLAY_MSG',
    'KILL_MSG',
    'STATUS_MSG',
    'ProcessLauncher',
    'create_continue_body',
    'create_launch_body',
    'RemoteProcessThreadController',
    'RemoteProcessController',
]

INTENT_KEY = 'intent'
MESSAGE_KEY = 'message'


class Intent(object):  # pylint: disable=useless-object-inheritance
    """Intent constants for a process message"""
    # pylint: disable=too-few-public-methods
    PLAY = 'play'
    PAUSE = 'pause'
    KILL = 'kill'
    STATUS = 'status'


PAUSE_MSG = {INTENT_KEY: Intent.PAUSE}
PLAY_MSG = {INTENT_KEY: Intent.PLAY}
KILL_MSG = {INTENT_KEY: Intent.KILL}
STATUS_MSG = {INTENT_KEY: Intent.STATUS}

TASK_KEY = 'task'
TASK_ARGS = 'args'
PERSIST_KEY = 'persist'
# Launch
PROCESS_CLASS_KEY = 'process_class'
ARGS_KEY = 'init_args'
KWARGS_KEY = 'init_kwargs'
NOWAIT_KEY = 'nowait'
# Continue
PID_KEY = 'pid'
TAG_KEY = 'tag'
# Task types
LAUNCH_TASK = 'launch'
CONTINUE_TASK = 'continue'
CREATE_TASK = 'create'

LOGGER = logging.getLogger(__name__)


def create_launch_body(process_class, init_args=None, init_kwargs=None, persist=False, loader=None, nowait=True):
    """
    Create a message body for the launch action

    :param process_class: the class of the process to launch
    :param init_args: any initialisation positional arguments
    :param init_kwargs: any initialisation keyword arguments
    :param persist: persist this process if True, otherwise don't
    :param loader: the loader to use to load the persisted process
    :param nowait: wait for the process to finish before completing the task, otherwise just return the PID
    :return: a dictionary with the body of the message to launch the process
    :rtype: dict
    """
    if loader is None:
        loader = loaders.get_object_loader()

    msg_body = {
        TASK_KEY: LAUNCH_TASK,
        TASK_ARGS: {
            PROCESS_CLASS_KEY: loader.identify_object(process_class),
            PERSIST_KEY: persist,
            NOWAIT_KEY: nowait,
            ARGS_KEY: init_args,
            KWARGS_KEY: init_kwargs
        }
    }
    return msg_body


def create_continue_body(pid, tag=None, nowait=False):
    """
    Create a message body to continue an existing process
    :param pid: the pid of the existing process
    :param tag: the optional persistence tag
    :param nowait: wait for the process to finish before completing the task, otherwise just return the PID
    :return: a dictionary with the body of the message to continue the process
    :rtype: dict
    """
    msg_body = {TASK_KEY: CONTINUE_TASK, TASK_ARGS: {PID_KEY: pid, NOWAIT_KEY: nowait, TAG_KEY: tag}}
    return msg_body


def create_create_body(process_class, init_args=None, init_kwargs=None, persist=False, loader=None):
    """
    Create a message body to create a new process
    :param process_class: the class of the process to launch
    :param init_args: any initialisation positional arguments
    :param init_kwargs: any initialisation keyword arguments
    :param persist: persist this process if True, otherwise don't
    :param loader: the loader to use to load the persisted process
    :return: a dictionary with the body of the message to launch the process
    :rtype: dict
    """
    if loader is None:
        loader = loaders.get_object_loader()

    msg_body = {
        TASK_KEY: CREATE_TASK,
        TASK_ARGS: {
            PROCESS_CLASS_KEY: loader.identify_object(process_class),
            PERSIST_KEY: persist,
            ARGS_KEY: init_args,
            KWARGS_KEY: init_kwargs
        }
    }
    return msg_body


class RemoteProcessController(object):  # pylint: disable=useless-object-inheritance
    """
    Control remote processes using coroutines that will send messages and wait
    (in a non-blocking way) for their response
    """

    def __init__(self, communicator):
        self._communicator = communicator

    @gen.coroutine
    def get_status(self, pid):
        """
        Get the status of a process with the given PID
        :param pid: the process id
        :return: the status response from the process
        """
        result = yield self._communicator.rpc_send(pid, STATUS_MSG)
        raise gen.Return(result)

    @gen.coroutine
    def pause_process(self, pid, msg=None):
        """
        Pause the process

        :param pid: the pid of the process to pause
        :param msg: optional pause message
        :return: True if paused, False otherwise
        """
        message = copy.copy(PAUSE_MSG)
        if msg is not None:
            message[MESSAGE_KEY] = msg

        pause_future = yield self._communicator.rpc_send(pid, message)
        result = yield pause_future
        raise gen.Return(result)

    @gen.coroutine
    def play_process(self, pid):
        """
        Play the process

        :param pid: the pid of the process to play
        :return: True if played, False otherwise
        """
        play_future = yield self._communicator.rpc_send(pid, PLAY_MSG)
        result = yield play_future
        raise gen.Return(result)

    @gen.coroutine
    def kill_process(self, pid, msg=None):
        """
        Kill the process

        :param pid: the pid of the process to kill
        :param msg: optional kill message
        :return: True if killed, False otherwise
        """
        message = copy.copy(KILL_MSG)
        if msg is not None:
            message[MESSAGE_KEY] = msg

        # Wait for the communication to go through
        kill_future = yield self._communicator.rpc_send(pid, message)
        # Now wait for the kill to be enacted
        result = yield kill_future

        raise gen.Return(result)

    @gen.coroutine
    def continue_process(self, pid, tag=None, nowait=False, no_reply=False):
        """
        Continue the process

        :param _communicator: the communicator
        :param pid: the pid of the process to continue
        :param tag: the checkpoint tag to continue from
        """
        message = create_continue_body(pid=pid, tag=tag, nowait=nowait)
        # Wait for the communication to go through
        continue_future = yield self._communicator.task_send(message, no_reply=no_reply)

        if no_reply:
            return

        # Now wait for the result of the task
        result = yield continue_future
        raise gen.Return(result)

    @gen.coroutine
    def launch_process(self,
                       process_class,
                       init_args=None,
                       init_kwargs=None,
                       persist=False,
                       loader=None,
                       nowait=False,
                       no_reply=False):
        """
        Launch a process given the class and constructor arguments

        :param process_class: the class of the process to launch
        :param init_args: the constructor positional arguments
        :param init_kwargs: the constructor keyword arguments
        :param persist: should the process be persisted
        :param loader: the classloader to use
        :param nowait: if True, don't wait for the process to send a response, just return the pid
        :param no_reply: if True, this call will be fire-and-forget, i.e. no return value
        :return: the result of launching the process
        """
        # pylint: disable=too-many-arguments
        message = create_launch_body(process_class, init_args, init_kwargs, persist, loader, nowait)
        launch_future = yield self._communicator.task_send(message, no_reply=no_reply)

        if no_reply:
            return

        result = yield launch_future
        raise gen.Return(result)

    @gen.coroutine
    def execute_process(self,
                        process_class,
                        init_args=None,
                        init_kwargs=None,
                        loader=None,
                        nowait=False,
                        no_reply=False):
        """
        Execute a process.  This call will first send a create task and then a continue task over
        the communicator.  This means that if communicator messages are durable then the process
        will run until the end even if this interpreter instance ceases to exist.

        :param process_class: the process class to execute
        :param init_args: the positional arguments to the class constructor
        :param init_kwargs: the keyword arguments to the class constructor
        :param loader: the class loader to use
        :param nowait: if True, don't wait for the process to send a response
        :param no_reply: if True, this call will be fire-and-forget, i.e. no return value
        :return: the result of executing the process
        """
        # pylint: disable=too-many-arguments
        message = create_create_body(process_class, init_args, init_kwargs, persist=True, loader=loader)

        create_future = yield self._communicator.task_send(message)
        pid = yield create_future

        message = create_continue_body(pid, nowait=nowait)
        continue_future = yield self._communicator.task_send(message, no_reply=no_reply)

        if no_reply:
            return

        result = yield continue_future
        raise gen.Return(result)


class RemoteProcessThreadController(object):  # pylint: disable=useless-object-inheritance
    """
    A class that can be used to control and launch remote processes
    """

    def __init__(self, communicator):
        """
        Create a new process controller

        :param communicator: the communicator to use
        :type communicator: :class:`kiwipy.Communicator`
        """
        self._communicator = communicator

    def get_status(self, pid):
        return self._communicator.rpc_send(pid, STATUS_MSG)

    def pause_process(self, pid, msg=None):
        """
        Pause the process

        :param pid: the pid of the process to pause
        :param msg: optional pause message
        :return: a response future from the process to be paused
        :rtype: :class:`kiwipy.Future`
        """
        message = copy.copy(PAUSE_MSG)
        if msg is not None:
            message[MESSAGE_KEY] = msg

        return self._communicator.rpc_send(pid, message)

    def pause_all(self, msg):
        """
        Pause all processes that are subscribed to the same communicator

        :param msg: an optional pause message
        """
        self._communicator.broadcast_send(msg, subject=Intent.PAUSE)

    def play_process(self, pid):
        """
        Play the process

        :param pid: the pid of the process to pause
        :return: a response future from the process to be played
        :rtype: :class:`kiwipy.Future`
        """
        return self._communicator.rpc_send(pid, PLAY_MSG)

    def play_all(self):
        """
        Play all processes that are subscribed to the same communicator
        """
        self._communicator.broadcast_send(None, subject=Intent.PLAY)

    def kill_process(self, pid, msg=None):
        """
        Kill the process

        :param pid: the pid of the process to kill
        :param msg: optional kill message
        :return: a response future from the process to be killed
        :rtype: :class:`kiwipy.Future`
        """
        message = copy.copy(KILL_MSG)
        if msg is not None:
            message[MESSAGE_KEY] = msg

        return self._communicator.rpc_send(pid, message)

    def kill_all(self, msg):
        """
        Kill all processes that are subscribed to the same communicator

        :param msg: an optional pause message
        """
        self._communicator.broadcast_send(msg, subject=Intent.KILL)

    def continue_process(self, pid, tag=None, nowait=False, no_reply=False):
        message = create_continue_body(pid=pid, tag=tag, nowait=nowait)
        return self.task_send(message, no_reply=no_reply)

    def launch_process(self,
                       process_class,
                       init_args=None,
                       init_kwargs=None,
                       persist=False,
                       loader=None,
                       nowait=False,
                       no_reply=False):
        # pylint: disable=too-many-arguments
        """
        Launch the process

        :param process_class: the process class to launch
        :param init_args: positional arguments to the process constructor
        :param init_kwargs: keyword arguments to the process constructor
        :param persist: should the process be persisted
        :param loader: the class loader to use
        :param nowait: if True only return when the process finishes
        :param no_reply: don't send a reply to the sender
        :return: the pid of the created process or the outputs (if nowait=False)
        """
        message = create_launch_body(process_class, init_args, init_kwargs, persist, loader, nowait)
        return self.task_send(message, no_reply=no_reply)

    def execute_process(self,
                        process_class,
                        init_args=None,
                        init_kwargs=None,
                        loader=None,
                        nowait=False,
                        no_reply=False):
        """
        Execute a process.  This call will first send a create task and then a continue task over
        the communicator.  This means that if communicator messages are durable then the process
        will run until the end even if this interpreter instance ceases to exist.

        :param process_class: the process class to execute
        :param init_args: the positional arguments to the class constructor
        :param init_kwargs: the keyword arguments to the class constructor
        :param loader: the class loader to use
        :param nowait: if True, don't wait for the process to send a response
        :param no_reply: if True, this call will be fire-and-forget, i.e. no return value
        :return: the result of executing the process
        """
        # pylint: disable=too-many-arguments
        message = create_create_body(process_class, init_args, init_kwargs, persist=True, loader=loader)

        execute_future = kiwipy.Future()
        create_future = futures.unwrap_kiwi_future(self._communicator.task_send(message))

        def on_created(_):
            with kiwipy.capture_exceptions(execute_future):
                pid = create_future.result()
                continue_future = self.continue_process(pid, nowait=nowait, no_reply=no_reply)
                kiwipy.chain(continue_future, execute_future)

        create_future.add_done_callback(on_created)
        return execute_future

    def task_send(self, message, no_reply=False):
        """
        Send a task to be performed using the communicator

        :param message: the task message
        :param no_reply: if True, this call will be fire-and-forget, i.e. no return value
        :return: the response from the remote side (if no_reply=False)
        """
        if no_reply:
            return self._communicator.task_send(message, no_reply=no_reply)

        return self._communicator.task_send(message, no_reply=no_reply)


class ProcessLauncher(object):  # pylint: disable=useless-object-inheritance
    """
    Takes incoming task messages and uses them to launch processes.

    Expected format of task:
    For launch:
    {
        'task': [LAUNCH_TASK]
        'process_class': [Process class to launch]
        'args': [tuple of positional args for process constructor]
        'kwargs': [dict of keyword args for process constructor].
        'nowait': True or False
    }

    For continue
    {
        'task': [CONTINUE_TASK]
        'pid': [Process ID]
        'nowait': True or False
    }
    """

    def __init__(self, loop=None, persister=None, load_context=None, loader=None):
        self._loop = loop
        self._persister = persister
        self._load_context = load_context if load_context is not None else persistence.LoadSaveContext()

        if loader is not None:
            self._loader = loader
            self._load_context = self._load_context.copyextend(loader=loader)
        else:
            self._loader = loaders.get_object_loader()

    @gen.coroutine
    def __call__(self, communicator, task):
        """
        Receive a task.
        :param task: The task message
        """
        task_type = task[TASK_KEY]
        if task_type == LAUNCH_TASK:
            raise gen.Return((yield self._launch(communicator, **task.get(TASK_ARGS, {}))))
        elif task_type == CONTINUE_TASK:
            raise gen.Return((yield self._continue(communicator, **task.get(TASK_ARGS, {}))))
        elif task_type == CREATE_TASK:
            raise gen.Return((yield self._create(communicator, **task.get(TASK_ARGS, {}))))
        else:
            raise communications.TaskRejected

    @gen.coroutine
    def _launch(self, _communicator, process_class, persist, nowait, init_args=None, init_kwargs=None):
        """
        Launch the process

        :param _communicator: the communicator
        :param process_class: the process class to launch
        :param persist: should the process be persisted
        :param nowait: if True only return when the process finishes
        :param init_args: positional arguments to the process constructor
        :param init_kwargs: keyword arguments to the process constructor
        :return: the pid of the created process or the outputs (if nowait=False)
        """
        if persist and not self._persister:
            raise communications.TaskRejected("Cannot persist process, no persister")

        if init_args is None:
            init_args = ()
        if init_kwargs is None:
            init_kwargs = {}

        proc_class = self._loader.load_object(process_class)
        proc = proc_class(*init_args, **init_kwargs)
        if persist:
            self._persister.save_checkpoint(proc)

        if nowait:
            self._loop.add_callback(proc.step_until_terminated)
            raise gen.Return(proc.pid)

        yield proc.step_until_terminated()
        raise gen.Return(proc.future().result())

    @gen.coroutine
    def _continue(self, _communicator, pid, nowait, tag=None):
        """
        Continue the process

        :param _communicator: the communicator
        :param pid: the pid of the process to continue
        :param nowait: if True don't wait for the process to complete
        :param tag: the checkpoint tag to continue from
        """
        if not self._persister:
            LOGGER.warning('rejecting task: cannot continue process<%d> because no persister is available', pid)
            raise communications.TaskRejected("Cannot continue process, no persister")

        # Do not catch exceptions here, because if these operations fail, the continue task should except and bubble up
        saved_state = self._persister.load_checkpoint(pid, tag)
        proc = saved_state.unbundle(self._load_context)

        if nowait:
            self._loop.add_callback(proc.step_until_terminated)
            raise gen.Return(proc.pid)

        yield proc.step_until_terminated()
        raise gen.Return(proc.future().result())

    @gen.coroutine
    def _create(self, _communicator, process_class, persist, init_args=None, init_kwargs=None):
        """
        Create the process

        :param _communicator: the communicator
        :param process_class: the process class to create
        :param persist: should the process be persisted
        :param init_args: positional arguments to the process constructor
        :param init_kwargs: keyword arguments to the process constructor
        :return: the pid of the created process
        """
        if persist and not self._persister:
            raise communications.TaskRejected("Cannot persist process, no persister")

        if init_args is None:
            init_args = ()
        if init_kwargs is None:
            init_kwargs = {}

        proc_class = self._loader.load_object(process_class)
        proc = proc_class(*init_args, **init_kwargs)
        if persist:
            self._persister.save_checkpoint(proc)

        raise gen.Return(proc.pid)
