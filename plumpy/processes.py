# -*- coding: utf-8 -*-
"""The main Process module"""

from __future__ import absolute_import
import abc
import contextlib
import functools
import copy
import logging
import time
import sys
import threading
import uuid
import six

from pika.exceptions import ConnectionClosed
from tornado import concurrent, gen
import tornado.stack_context
import yaml

import kiwipy

from .process_listener import ProcessListener
from .process_spec import ProcessSpec
from .utils import protected
from . import exceptions
from . import futures
from . import base
from .base import state_machine
from .base import super_check, call_with_super_check
from .base.state_machine import StateEntryFailed, StateMachine, event
from .base import TransitionFailed
from . import events
from . import persistence
from . import process_comms
from . import process_states
from . import ports
from . import utils

# pylint: disable=too-many-lines

__all__ = ['Process', 'ProcessSpec', 'BundleKeys', 'TransitionFailed']

_LOGGER = logging.getLogger(__name__)


class BundleKeys(object):
    """
    String keys used by the process to save its state in the state bundle.

    See :func:`save_instance_state` and :func:`load_instance_state`.
    """
    # pylint: disable=too-few-public-methods
    INPUTS_RAW = 'INPUTS_RAW'
    INPUTS_PARSED = 'INPUTS_PARSED'
    OUTPUTS = 'OUTPUTS'


# Use thread-local storage for the stack
_thread_local = threading.local()  # pylint: disable=invalid-name


def _process_stack():
    """Access the private live stack"""
    global _thread_local  # pylint: disable=global-statement, invalid-name
    # Lazily create the first time it's used
    try:
        return _thread_local.process_stack
    except AttributeError:
        _thread_local.process_stack = []
        return _thread_local.process_stack


class ProcessStateMachineMeta(abc.ABCMeta, state_machine.StateMachineMeta):
    pass


# Make ProcessStateMachineMeta instances (classes) YAML - able
yaml.representer.Representer.add_representer(ProcessStateMachineMeta, yaml.representer.Representer.represent_name)


def ensure_not_closed(func):

    @functools.wraps(func)
    def func_wrapper(self, *args, **kwargs):
        if self._closed:
            raise exceptions.ClosedError("Process is closed")
        return func(self, *args, **kwargs)

    return func_wrapper


@six.add_metaclass(ProcessStateMachineMeta)
@persistence.auto_persist('_pid', '_CREATION_TIME', '_future', '_paused', '_status', '_pre_paused_status')
class Process(StateMachine, persistence.Savable):
    """
    The Process class is the base for any unit of work in plumpy.

    A process can be in one of the following states:

    * CREATED
    * RUNNING
    * WAITING
    * FINISHED
    * EXCEPTED
    * KILLED

    as defined in the :class:`ProcessState` enum.

    ::

                          ___
                         |   v
        CREATED (x) --- RUNNING (x) --- FINISHED (o)
                         |   ^          /
                         v   |         /
                        WAITING (x) --
                         |   ^
                          ---

        * -- EXCEPTED (o)
        * -- KILLED (o)

    * (o): terminal state
    * (x): non terminal state

    When a Process enters a state is always gets a corresponding message, e.g.
    on entering RUNNING it will receive the on_run message. These are
    always called immediately after that state is entered but before being
    executed.
    """

    # Static class stuff ######################
    _spec_class = ProcessSpec
    # Default placeholders, will be populated in init()
    _stepping = False
    _pausing = None  # type: futures.Future
    _paused = None
    _killing = None
    _interrupt_action = None
    _closed = False
    _cleanups = None  # type: list

    @classmethod
    def current(cls):
        """
        Get the currently running process i.e. the one at the top of the stack

        :return: the currently running process
        :rtype: :class:`plumpy.Process`
        """
        if _process_stack():
            return _process_stack()[-1]

        return None

    @classmethod
    def get_states(cls):
        state_classes = cls.get_state_classes()
        return (state_classes[process_states.ProcessState.CREATED],) + \
               tuple(state
                     for state in state_classes.values()
                     if state.LABEL != process_states.ProcessState.CREATED)

    @classmethod
    def get_state_classes(cls):
        # A mapping of the State constants to the corresponding state class
        return {
            process_states.ProcessState.CREATED: process_states.Created,
            process_states.ProcessState.RUNNING: process_states.Running,
            process_states.ProcessState.WAITING: process_states.Waiting,
            process_states.ProcessState.FINISHED: process_states.Finished,
            process_states.ProcessState.EXCEPTED: process_states.Excepted,
            process_states.ProcessState.KILLED: process_states.Killed
        }

    @classmethod
    def spec(cls):
        try:
            return cls.__getattribute__(cls, '_spec')
        except AttributeError:
            cls._spec = cls._spec_class()
            cls.__called = False
            cls.define(cls._spec)
            assert cls.__called, \
                "Process.define() was not called by {}\n" \
                "Hint: Did you forget to call the superclass method in your define? " \
                "Try: super({}, cls).define(spec)".format(cls, cls.__name__)
            return cls._spec

    @classmethod
    def get_name(cls):
        return cls.__name__

    @classmethod
    def define(cls, _spec):
        cls.__called = True

    @classmethod
    def get_description(cls):
        """
        Get a human readable description of what this :class:`Process` does.

        :return: The description.
        :rtype: dict
        """
        description = {}

        if cls.__doc__:
            description['description'] = cls.__doc__.strip()

        spec_description = cls.spec().get_description()
        if spec_description:
            description['spec'] = spec_description

        return description

    @classmethod
    def recreate_from(cls, saved_state, load_context=None):
        """
        Recreate a process from a saved state, passing any positional and
        keyword arguments on to load_instance_state

        :param saved_state: The saved state to load from
        :param load_context: The load context to use
        :type load_context: :class:`persistence.LoadSaveContext`
        :return: An instance of the object with its state loaded from the save state.
        :rtype: :class:`Process`
        """
        process = super(Process, cls).recreate_from(saved_state, load_context)
        base.call_with_super_check(process.init)
        return process

    def __init__(self, inputs=None, pid=None, logger=None, loop=None, communicator=None):
        """
        The signature of the constructor should not be changed by subclassing
        processes.

        :param inputs: A dictionary of the process inputs
        :type inputs: dict
        :param pid: The process ID, can be manually set, if not a unique pid
            will be chosen
        :param logger: An optional logger for the process to use
        :type logger: :class:`logging.Logger`
        :param loop: The event loop
        :param communicator: The (optional) communicator
        :type communicator: :class:`plumpy.Communicator`
        """
        super(Process, self).__init__()

        # Don't allow the spec to be changed anymore
        self.spec().seal()

        self._loop = loop if loop is not None else events.get_event_loop()

        self._setup_event_hooks()

        self._status = None  # May hold a current status message
        self._pre_paused_status = None  # Save status when a pause message replaces it, such that it can be restored
        self._paused = None

        # Input/output
        self._raw_inputs = None if inputs is None else utils.AttributesFrozendict(inputs)
        self._pid = pid
        self._parsed_inputs = None
        self._outputs = {}
        self._uuid = None
        self._CREATION_TIME = None

        # Runtime variables
        self._future = persistence.SavableFuture()
        self.__event_helper = utils.EventHelper(ProcessListener)
        self._logger = logger
        self._communicator = communicator

    @base.super_check
    def init(self):
        """ Any common initialisation stuff after create or load goes here """
        self._cleanups = []  # a list of functions to be ran on terminated

        if self._communicator is not None:
            try:
                identifier = self._communicator.add_rpc_subscriber(self.message_receive, identifier=str(self.pid))
                self.add_cleanup(functools.partial(self._communicator.remove_rpc_subscriber, identifier))
            except kiwipy.TimeoutError:
                self.logger.exception("Process<%s> failed to register as an RPC subscriber", self.pid)

            try:
                identifier = self._communicator.add_broadcast_subscriber(
                    self.broadcast_receive, identifier=str(self.pid))
                self.add_cleanup(functools.partial(self._communicator.remove_broadcast_subscriber, identifier))
            except kiwipy.TimeoutError:
                self.logger.exception("Process<%s> failed to register as a broadcast subscriber", self.pid)

        if not self._future.done():

            def try_killing(future):
                if future.cancelled():
                    if not self.kill('Killed by future being cancelled'):
                        self.logger.warning("Failed to kill process on future cancel")

            self._future.add_done_callback(try_killing)

    def _setup_event_hooks(self):
        self.add_state_event_callback(state_machine.StateEventHook.ENTERING_STATE,
                                      lambda _s, _h, state: self.on_entering(state))
        self.add_state_event_callback(state_machine.StateEventHook.ENTERED_STATE,
                                      lambda _s, _h, from_state: self.on_entered(from_state))
        self.add_state_event_callback(state_machine.StateEventHook.EXITING_STATE,
                                      lambda _s, _h, _state: self.on_exiting())

    @property
    def creation_time(self):
        """
        The creation time of this Process as returned by time.time() when instantiated
        :return: The creation time
        :rtype: float
        """
        return self._CREATION_TIME

    @property
    def pid(self):
        return self._pid

    @property
    def uuid(self):
        return self._uuid

    @property
    def raw_inputs(self):
        return self._raw_inputs

    @property
    def inputs(self):
        return self._parsed_inputs

    @property
    def outputs(self):
        """
        Get the current outputs emitted by the Process.  These may grow over
        time as the process runs.

        :return: A mapping of {output_port: value} outputs
        :rtype: dict
        """
        return self._outputs

    @property
    def logger(self):
        """
        Get the logger for this class.  Can be None.

        :return: The logger.
        :rtype: :class:`logging.Logger`
        """
        if self._logger is not None:
            return self._logger

        return _LOGGER

    @property
    def status(self):
        return self._status

    def set_status(self, status):
        self._status = status

    @property
    def paused(self):
        return self._paused is not None

    def future(self):
        return self._future

    @ensure_not_closed
    def launch(self, process_class, inputs=None, pid=None, logger=None):
        process = process_class(
            inputs=inputs, pid=pid, logger=logger, loop=self.loop(), communicator=self._communicator)
        self.loop().add_callback(process.step_until_terminated)
        return process

    # region State introspection methods

    def has_terminated(self):
        return self._state.is_terminal()

    def result(self):
        """
        Get the result from the process if it is finished.
        If the process was killed then a KilledError will be raise.
        If the process has excepted then the failing exception will be raised.
        If in any other state this will raise an InvalidStateError.
        :return: The result of the process
        """
        if isinstance(self._state, process_states.Finished):
            return self._state.result
        if isinstance(self._state, process_states.Killed):
            raise exceptions.KilledError()
        if isinstance(self._state, process_states.Excepted):
            raise self._state.exception

        raise exceptions.InvalidStateError

    def successful(self):
        """
        Returns whether the result of the process is considered successful
        Will raise if the process is not in the FINISHED state
        """
        try:
            return self._state.successful
        except AttributeError:
            raise exceptions.InvalidStateError('process is not in the finished state')

    @property
    def is_successful(self):
        """Return whether the result of the process is considered successful.

        :return: boolean, True if the process is in `Finished` state with `successful` attribute set to `True`
        """
        try:
            return self._state.successful
        except AttributeError:
            return False

    def killed(self):
        return self.state == process_states.ProcessState.KILLED

    def killed_msg(self):
        if isinstance(self._state, process_states.Killed):
            return self._state.msg

        raise exceptions.InvalidStateError('Has not been killed')

    def exception(self):
        if isinstance(self._state, process_states.Excepted):
            return self._state.exception

        return None

    def done(self):
        """
        Return True if the call was successfully killed or finished running.
        :rtype: bool
        """
        return self._state.is_terminal()

    # endregion

    # region loop methods

    def loop(self):
        return self._loop

    def call_soon(self, callback, *args, **kwargs):
        """
        Schedule a callback to what is considered an internal process function
        (this needn't be a method).  If it raises an exception it will cause
        the process to fail.
        """
        args = (callback,) + args
        handle = events.ProcessCallback(self, self._run_task, args, kwargs)
        self._loop.add_callback(handle.run)
        return handle

    def callback_excepted(self, _callback, exception, trace):
        if self.state != process_states.ProcessState.EXCEPTED:
            self.fail(exception, trace)

    @contextlib.contextmanager
    def _process_scope(self):
        """
        This context manager function is used to make sure the process stack is correct
        meaning that globally someone can ask for Process.current() to get the last process
        that is on the call stack.
        """
        _process_stack().append(self)
        try:
            yield
        finally:
            assert Process.current() is self, \
                "Somehow, the process at the top of the stack is not me, " \
                "but another process! ({} != {})".format(self, Process.current())
            _process_stack().pop()

    @gen.coroutine
    def _run_task(self, callback, *args, **kwargs):
        """
        This method should be used to run all Process related functions and coroutines.
        If there is an exception the process will enter the EXCEPTED state.

        :param callback: A function or coroutine
        :param args: Optional positional arguments passed to fn
        :param kwargs:  Optional keyword arguments passed to fn
        :return: The value as returned by fn
        """
        # Make sure execute is a coroutine
        coro = utils.ensure_coroutine(callback)
        result = yield tornado.stack_context.run_with_stack_context(
            tornado.stack_context.StackContext(self._process_scope), functools.partial(coro, *args, **kwargs))
        raise gen.Return(result)

    # endregion

    # region Persistence

    def save_instance_state(self, out_state, save_context):
        """
        Ask the process to save its current instance state.

        :param out_state: A bundle to save the state to
        :type out_state: :class:`plumpy.Bundle`
        :param save_context: The save context
        """
        super(Process, self).save_instance_state(out_state, save_context)

        out_state['_state'] = self._state.save()

        # Inputs/outputs
        if self.raw_inputs is not None:
            out_state[BundleKeys.INPUTS_RAW] = self.encode_input_args(self.raw_inputs)

        if self.inputs is not None:
            out_state[BundleKeys.INPUTS_PARSED] = self.encode_input_args(self.inputs)

        if self.outputs:
            out_state[BundleKeys.OUTPUTS] = self.encode_input_args(self.outputs)

    @protected
    def load_instance_state(self, saved_state, load_context):
        # First make sure the state machine constructor is called
        super(Process, self).__init__()

        self._setup_event_hooks()

        # Runtime variables, set initial states
        self._future = persistence.SavableFuture()
        self.__event_helper = utils.EventHelper(ProcessListener)
        self._logger = None
        self._communicator = None

        if 'loop' in load_context:
            self._loop = load_context.loop
        else:
            self._loop = events.get_event_loop()

        self._state = self.recreate_state(saved_state['_state'])

        if 'communicator' in load_context:
            self._communicator = load_context.communicator

        if 'logger' in load_context:
            self._logger = load_context.logger

        # Need to call this here as things downstream may rely on us having the runtime variable above
        super(Process, self).load_instance_state(saved_state, load_context)

        # Inputs/outputs
        try:
            decoded = self.decode_input_args(saved_state[BundleKeys.INPUTS_RAW])
            self._raw_inputs = utils.AttributesFrozendict(decoded)
        except KeyError:
            self._raw_inputs = None

        try:
            decoded = self.decode_input_args(saved_state[BundleKeys.INPUTS_PARSED])
            self._parsed_inputs = utils.AttributesFrozendict(decoded)
        except KeyError:
            self._parsed_inputs = None

        try:
            decoded = self.decode_input_args(saved_state[BundleKeys.OUTPUTS])
            self._outputs = decoded
        except KeyError:
            self._outputs = {}

    # endregion

    def add_process_listener(self, listener):
        assert (listener != self), "Cannot listen to yourself!"
        self.__event_helper.add_listener(listener)

    def remove_process_listener(self, listener):
        self.__event_helper.remove_listener(listener)

    @protected
    def set_logger(self, logger):
        self._logger = logger

    @protected
    def log_with_pid(self, level, msg):
        self.logger.log(level, "%s: %s", self.pid, msg)

    # region Events

    def on_entering(self, state):
        # Map these onto direct functions that the subclass can implement
        state_label = state.LABEL
        if state_label == process_states.ProcessState.CREATED:
            call_with_super_check(self.on_create)
        elif state_label == process_states.ProcessState.RUNNING:
            call_with_super_check(self.on_run)
        elif state_label == process_states.ProcessState.WAITING:
            call_with_super_check(self.on_wait, state.data)
        elif state_label == process_states.ProcessState.FINISHED:
            call_with_super_check(self.on_finish, state.result, state.successful)
        elif state_label == process_states.ProcessState.KILLED:
            call_with_super_check(self.on_kill, state.msg)
        elif state_label == process_states.ProcessState.EXCEPTED:
            call_with_super_check(self.on_except, state.get_exc_info())

    def on_entered(self, from_state):
        # Map these onto direct functions that the subclass can implement
        state_label = self._state.LABEL
        if state_label == process_states.ProcessState.RUNNING:
            call_with_super_check(self.on_running)
        elif state_label == process_states.ProcessState.WAITING:
            call_with_super_check(self.on_waiting)
        elif state_label == process_states.ProcessState.FINISHED:
            call_with_super_check(self.on_finished)
        elif state_label == process_states.ProcessState.EXCEPTED:
            call_with_super_check(self.on_excepted)
        elif state_label == process_states.ProcessState.KILLED:
            call_with_super_check(self.on_killed)

        if self._communicator:
            from_label = from_state.LABEL.value if from_state is not None else None
            try:
                self._communicator.broadcast_send(
                    body=None, sender=self.pid, subject='state_changed.{}.{}'.format(from_label, self.state.value))
            except ConnectionClosed:
                self.logger.info('no connection available to broadcast state change from %s to %s', from_label,
                                 self.state.value)

    def on_exiting(self):
        state = self.state
        if state == process_states.ProcessState.WAITING:
            call_with_super_check(self.on_exit_waiting)
        elif state == process_states.ProcessState.RUNNING:
            call_with_super_check(self.on_exit_running)

    @super_check
    def on_create(self):
        self._CREATION_TIME = time.time()

        # This will parse the inputs with respect to the input portnamespace of the spec and validate them
        raw_inputs = dict(self._raw_inputs) if self._raw_inputs else {}
        self._parsed_inputs = self.spec().inputs.pre_process(raw_inputs)
        result = self.spec().inputs.validate(self._parsed_inputs)

        if result is not None:
            raise ValueError(result)

        # Set up a process ID
        self._uuid = uuid.uuid4()
        if self._pid is None:
            self._pid = self._uuid

    @super_check
    def on_exit_running(self):
        pass

    @super_check
    def on_exit_waiting(self):
        pass

    @super_check
    def on_run(self):
        """ Entering the RUNNING state """
        pass

    @super_check
    def on_running(self):
        """ Entered the RUNNING state """
        self._fire_event(ProcessListener.on_process_running)

    def on_output_emitting(self, output_port, value):
        pass

    def on_output_emitted(self, output_port, value, dynamic):
        self.__event_helper.fire_event(ProcessListener.on_output_emitted, self, output_port, value, dynamic)

    @super_check
    def on_wait(self, awaitables):
        """ Entering the WAITING state """
        pass

    @super_check
    def on_waiting(self):
        """ Entered the WAITING state """
        self._fire_event(ProcessListener.on_process_waiting)

    @super_check
    def on_pausing(self, msg=None):
        """ The process is being paused """
        pass

    @super_check
    def on_paused(self, msg=None):
        """ The process was paused """
        self._pausing = None

        # Create a future to represent the duration of the paused state
        self._paused = persistence.SavableFuture()

        # Save the current status and potentially overwrite it with the passed message
        self._pre_paused_status = self.status
        if msg is not None:
            self.set_status(msg)

        self._fire_event(ProcessListener.on_process_paused)

    @super_check
    def on_playing(self):
        """ The process was played """
        # Done being paused
        self._paused.set_result(True)
        self._paused = None

        self.set_status(self._pre_paused_status)
        self._pre_paused_status = None

        self._fire_event(ProcessListener.on_process_played)

    @super_check
    def on_finish(self, result, successful):
        """ Entering the FINISHED state """
        if successful:
            validation_error = self.spec().outputs.validate(self.outputs)
            if validation_error:
                raise StateEntryFailed(process_states.ProcessState.FINISHED, result, False)

        self.future().set_result(self.outputs)

    @super_check
    def on_finished(self):
        """ Entered the FINISHED state """
        self._fire_event(ProcessListener.on_process_finished, self.future().result())

    @super_check
    def on_except(self, exc_info):
        self.future().set_exception(exc_info[1])

    @super_check
    def on_excepted(self):
        self._fire_event(ProcessListener.on_process_excepted, str(self.future().exception()))

    @super_check
    def on_kill(self, msg):
        self.set_status(msg)
        self.future().set_exception(exceptions.KilledError(msg))

    @super_check
    def on_killed(self):
        self._killing = None
        self._fire_event(ProcessListener.on_process_killed, self.killed_msg())

    def on_terminated(self):
        super(Process, self).on_terminated()
        self.close()

    @super_check
    def on_close(self):
        """
        Called when the Process is being closed an will not be ran anymore.  This is an opportunity
        to free any runtime resources
        """
        try:
            for cleanup in self._cleanups:
                try:
                    cleanup()
                except Exception:  # pylint: disable=broad-except
                    self.logger.exception('Exception calling cleanup method %s', cleanup)
            self._cleanups = None
        finally:
            self._closed = True

    def _fire_event(self, evt, *args, **kwargs):
        self.__event_helper.fire_event(evt, self, *args, **kwargs)

    # endregion

    # region Communication

    def message_receive(self, _comm, msg):
        """
        Coroutine called when the process receives a message from the communicator

        :param _comm: the communicator that sent the message
        :type _comm: :class:`kiwipy.Communicator`
        :param msg: the message
        :return: the outcome of processing the message, the return value will be sent back as a response to the sender
        """
        self.logger.debug("RPC message '%s' received with communicator '%s'", msg, _comm)

        intent = msg[process_comms.INTENT_KEY]

        if intent == process_comms.Intent.PLAY:
            return self._schedule_rpc(self.play)
        if intent == process_comms.Intent.PAUSE:
            return self._schedule_rpc(self.pause, msg=msg.get(process_comms.MESSAGE_KEY, None))
        if intent == process_comms.Intent.KILL:
            return self._schedule_rpc(self.kill, msg=msg.get(process_comms.MESSAGE_KEY, None))
        if intent == process_comms.Intent.STATUS:
            status_info = {}
            self.get_status_info(status_info)
            return status_info

        # Didn't match any known intents
        raise RuntimeError("Unknown intent")

    def broadcast_receive(self, _comm, body, sender, subject, correlation_id):
        """
        Coroutine called when the process receives a message from the communicator

        :param _comm: the communicator that sent the message
        :type _comm: :class:`kiwipy.Communicator`
        :param msg: the message
        """
        self.logger.debug("Broadcast message '%s' received with communicator '%s'", body, _comm)

        # If we get a message we recognise then action it, otherwise ignore
        if subject == process_comms.Intent.PLAY:
            return self._schedule_rpc(self.play)
        if subject == process_comms.Intent.PAUSE:
            return self._schedule_rpc(self.pause, msg=body)
        if subject == process_comms.Intent.KILL:
            return self._schedule_rpc(self.kill, msg=body)

        return

    def _schedule_rpc(self, callback, *args, **kwargs):
        """
        Schedule a call to a callback as a result of an RPC communication call, this will return
        a future that resolves to the final result (even after one or more layer of futures being
        returned) of the callback.

        :param callback: the callback function or coroutine
        :param args: the positional arguments to the callback
        :param kwargs: the keyword arguments to the callback
        :return: a kiwi future that resolves to the outcome of the callback
        :rtype: :class:`kiwipy.Future`
        """
        kiwi_future = kiwipy.Future()

        @gen.coroutine
        def run_callback():
            with kiwipy.capture_exceptions(kiwi_future):
                result = callback(*args, **kwargs)
                while concurrent.is_future(result):
                    result = yield result

                kiwi_future.set_result(result)

        # Schedule the task and give back a kiwi future
        self.loop().add_callback(run_callback)
        return kiwi_future

    # endregion

    @ensure_not_closed
    def add_cleanup(self, cleanup):
        self._cleanups.append(cleanup)

    def close(self):
        """
        Calling this method indicates that this process should not ran anymore and will trigger
        any runtime resources (such as the communicator connection) to be cleaned up.  The state
        of the process will still be accessible.

        It is safe to call this method multiple times.
        """
        if self._closed:
            return

        call_with_super_check(self.on_close)

    # region State related methods

    def transition_excepted(self, _initial_state, final_state, exception, trace):
        # If we are creating, then reraise instead of failing.
        if final_state == process_states.ProcessState.CREATED:
            six.reraise(type(exception), exception, trace)
        else:
            self.transition_to(process_states.ProcessState.EXCEPTED, exception, trace)

    def pause(self, msg=None):
        """
        Pause the process.  Returns True if after this call the process is paused, False otherwise

        :param msg: an optional message to set as the status. The current status will be saved in the private
            `_pre_paused_status attribute`, such that it can be restored when the process is played again.
        :return: True paused, False otherwise
        """
        if self.has_terminated():
            return False

        if self.paused:
            # Already paused
            return True

        if self._pausing is not None:
            # Already pausing
            return self._pausing

        if self._stepping:
            # Ask the step function to pause by setting this flag and giving the
            # caller back a future
            interrupt_exception = process_states.PauseInterruption(msg)
            self._set_interrupt_action_from_exception(interrupt_exception)
            self._pausing = self._interrupt_action
            # Try to interrupt the state
            self._state.interrupt(interrupt_exception)
            return self._interrupt_action

        return self._do_pause(msg)

    def _do_pause(self, state_msg, next_state=None):
        """ Carry out the pause procedure, optionally transitioning to the next state first"""
        try:
            if next_state is not None:
                self.transition_to(next_state)
            call_with_super_check(self.on_pausing, state_msg)
            call_with_super_check(self.on_paused, state_msg)
        finally:
            self._pausing = None

        return True

    def _create_interrupt_action(self, exception):
        """
        Create an interrupt action from the corresponding interrupt exception
        :param exception: The interrupt exception
        :type exception: :class:`plumpy.InterruptException`
        :return: The interrupt action
        :rtype: :class:`plumpy.CancellableAction`
        """
        if isinstance(exception, process_states.PauseInterruption):
            do_pause = functools.partial(self._do_pause, str(exception))
            return futures.CancellableAction(do_pause, cookie=exception)

        if isinstance(exception, process_states.KillInterruption):

            def do_kill(_next_state):
                try:
                    # Ignore the next state
                    self.transition_to(process_states.ProcessState.KILLED, str(exception))
                    return True
                finally:
                    self._killing = None

            return futures.CancellableAction(do_kill, cookie=exception)

        raise ValueError("Got unknown interruption type '{}'".format(type(exception)))

    def _set_interrupt_action(self, new_action):
        """
        Set the interrupt action cancelling the current one if it exists
        :param new_action: The new interrupt action to set
        """
        if self._interrupt_action is not None:
            self._interrupt_action.cancel()
        self._interrupt_action = new_action

    def _set_interrupt_action_from_exception(self, interrupt_exception):
        """ Set an interrupt action from the corresponding interrupt exception """
        action = self._create_interrupt_action(interrupt_exception)
        self._set_interrupt_action(action)

    def play(self):
        """
        Play a process. Returns True if after this call the process is playing, False otherwise

        :return: True if playing, False otherwise
        """
        if not self.paused:
            if self._pausing is not None:
                # Not going to pause after all
                self._pausing.cancel()
                self._pausing = None
                self._set_interrupt_action(None)
            return True

        call_with_super_check(self.on_playing)
        return True

    @event(from_states=(process_states.Running, process_states.Waiting))
    def resume(self, *args):
        """Start running the process again"""
        return self._state.resume(*args)

    @event(to_states=process_states.Excepted)
    def fail(self, exception, trace_back=None):
        """
        Fail the process in response to an exception
        :param exception: The exception that caused the failure
        :param trace_back: Optional exception traceback
        """
        self.transition_to(process_states.ProcessState.EXCEPTED, exception, trace_back)

    def kill(self, msg=None):
        """
        Kill the process
        :param msg: An optional kill message
        :type msg: str
        """
        if self.state == process_states.ProcessState.KILLED:
            # Already killed
            return True

        if self.has_terminated():
            # Can't kill
            return False

        if self._killing:
            # Already killing
            return self._killing

        if self._stepping:
            # Ask the step function to pause by setting this flag and giving the
            # caller back a future
            interrupt_exception = process_states.KillInterruption(msg)
            self._set_interrupt_action_from_exception(interrupt_exception)
            self._killing = self._interrupt_action
            self._state.interrupt(interrupt_exception)
            return self._interrupt_action

        self.transition_to(process_states.ProcessState.KILLED, msg)
        return True

        # endregion

    def create_initial_state(self):
        return self.get_state_class(process_states.ProcessState.CREATED)(self, self.run)

    def recreate_state(self, saved_state):
        """
        Create a state object from a saved state

        :param saved_state: The saved state
        :type saved_state: :class:`Bundle`
        :return: An instance of the object with its state loaded from the save state.
        """
        load_context = persistence.LoadSaveContext(process=self)
        return persistence.Savable.load(saved_state, load_context)

    # endregion

    # region Execution related methods

    def run(self):
        pass

    @ensure_not_closed
    def execute(self):
        """
        Execute the process.  This will return if the process terminates or is paused.

        :return: None if not terminated, otherwise `self.outputs`
        """
        if not self.has_terminated():
            self.loop().run_sync(self.step_until_terminated)

        return self.future().result()

    @ensure_not_closed
    @gen.coroutine
    def step(self):
        assert not self.has_terminated(), "Cannot step, already terminated"

        if self.paused:
            yield self._paused

        try:
            self._stepping = True
            next_state = None
            try:
                next_state = yield self._run_task(self._state.execute)
            except process_states.Interruption as exception:
                # If the interruption was caused by a call to a Process method then there should
                # be an interrupt action ready to be executed, so just check if the cookie matches
                # that of the exception i.e. if it is the _same_ interruption.  If not cancel and
                # build the interrupt action below
                if self._interrupt_action is not None:
                    if self._interrupt_action.cookie is not exception:
                        self._set_interrupt_action_from_exception(exception)
                else:
                    self._set_interrupt_action_from_exception(exception)

            except KeyboardInterrupt:  # pylint: disable=try-except-raise
                raise
            except Exception:  # pylint: disable=broad-except
                # Overwrite the next state to go to excepted directly
                exc_info = sys.exc_info()
                next_state = self.create_state(process_states.ProcessState.EXCEPTED, exc_info[1], exc_info[2])
                self._set_interrupt_action(None)

            if self._interrupt_action:
                self._interrupt_action.run(next_state)
            else:
                # Everything nominal so transition to the next state
                self.transition_to(next_state)

        finally:
            self._stepping = False
            self._set_interrupt_action(None)

    @gen.coroutine
    def step_until_terminated(self):
        while not self.has_terminated():
            yield self.step()

    # endregion

    @ensure_not_closed
    @protected
    def out(self, output_port, value):
        """
        Record an output value for a specific output port. If the output port matches an
        explicitly defined Port it will be validated against that. If not it will be validated
        against the PortNamespace, which means it will be checked for dynamicity and whether
        the type of the value is valid

        :param output_port: the name of the output port, can be namespaced
        :type output_port: str
        :param value: the value for the output port
        :raises: ValueError if the output value is not validated against the port
        """
        self.on_output_emitting(output_port, value)

        namespace_separator = self.spec().namespace_separator

        namespace = output_port.split(namespace_separator)
        port_name = namespace.pop()

        if namespace:
            port_namespace = self.spec().outputs.get_port(namespace_separator.join(namespace))
        else:
            port_namespace = self.spec().outputs

        validation_error = None
        try:
            port = port_namespace[port_name]
            dynamic = False
            validation_error = port.validate(value)
        except KeyError:
            port = port_namespace
            dynamic = True
            validation_error = port.validate_dynamic_ports({port_name: value})

        if validation_error:
            msg = "Error validating output '{}' for port '{}': {}".format(value, validation_error.port,
                                                                          validation_error.message)
            raise ValueError(msg)

        output_namespace = self._outputs
        for sub_space in namespace:
            output_namespace = output_namespace.setdefault(sub_space, {})

        output_namespace[port_name] = value
        self.on_output_emitted(output_port, value, dynamic)

    @protected
    def encode_input_args(self, inputs):
        """
        Encode input arguments such that they may be saved in a :class:`plumpy.Bundle`.
        The encoded inputs should contain no reference to the inputs that were passed in.
        This often will mean making a deepcopy of the input dictionary.

        :param inputs: A mapping of the inputs as passed to the process
        :return: The encoded inputs
        """
        # pylint: disable=no-self-use
        return copy.deepcopy(inputs)

    @protected
    def decode_input_args(self, encoded):
        """
        Decode saved input arguments as they came from the saved instance state :class:`plumpy.Bundle`.
        The decoded inputs should contain no reference to the encoded inputs that were passed in.
        This often will mean making a deepcopy of the encoded input dictionary.

        :param encoded:
        :return: The decoded input args
        """
        # pylint: disable=no-self-use
        return copy.deepcopy(encoded)

    def get_status_info(self, out_status_info):
        out_status_info.update({
            'ctime': self.creation_time,
            'paused': self.paused,
            'process_string': str(self),
            'state': self.state,
            'state_info': str(self._state)
        })
