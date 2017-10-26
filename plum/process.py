# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
import apricotpy
import apricotpy.persistable
from collections import namedtuple
import copy
from enum import Enum
import inspect
import logging
import time
import sys

from future.utils import with_metaclass

import plum.stack as _stack
from plum.process_listener import ProcessListener
from plum.process_spec import ProcessSpec
from plum.utils import protected
from plum.port import _NULL
from . import utils

__all__ = ['Process', 'ProcessState', 'get_pid_from_bundle']

_LOGGER = logging.getLogger(__name__)


class ProcessState(Enum):
    """
    The possible states that a :class:`Process` can be in.
    """
    CREATED = 0
    RUNNING = 1
    WAITING = 2
    STOPPED = 3
    FAILED = 4


Wait = namedtuple('Wait', ['on', 'callback'])


def _should_pass_result(fn):
    if isinstance(fn, apricotpy.persistable.Function):
        fn = fn._fn

    fn_spec = inspect.getargspec(fn)
    is_method_with_argument = inspect.ismethod(fn) and len(fn_spec[0]) > 1
    is_function_with_argument = inspect.isfunction(fn) and len(fn_spec[0]) > 0
    has_args_or_kwargs = fn_spec[1] is not None or fn_spec[2] is not None
    return is_method_with_argument or is_function_with_argument or has_args_or_kwargs


class BundleKeys(Enum):
    """
    String keys used by the process to save its state in the state bundle.

    See :func:`create_from`, :func:`save_instance_state` and :func:`load_instance_state`.
    """
    CREATION_TIME = 'creation_time'
    INPUTS = 'inputs'
    OUTPUTS = 'outputs'
    PID = 'pid'
    LOOP_CALLBACK = 'LOOP_CALLBACK'
    AWAITING = 'AWAITING'
    NEXT_STEP = 'NEXT_STEP'
    ABORT_MSG = 'ABORT_MSG'
    PROC_STATE = 'PROC_SATE'
    PAUSED = 'PAUSED'
    CALLBACK_FN = 'CALLBACK_FN'
    CALLBACK_ARGS = 'CALLBACK_ARGS'


class Process(with_metaclass(ABCMeta, apricotpy.persistable.AwaitableLoopObject)):
    """
    The Process class is the base for any unit of work in plum.

    Once a process is created it may be started by calling play() at which
    point it is said to be 'playing', like a tape.  It can then be paused by
    calling pause() which will only be acted on at the next state transition
    OR if the process is in the WAITING state in which case it will pause
    immediately.  It can be resumed with a call to play().

    A process can be in one of the following states:

    * CREATED
    * RUNNING
    * WAITING
    * STOPPED
    * FAILED

    as defined in the :class:`ProcessState` enum.

    The possible transitions between states are::

                              _(reenter)_
                              |         |
        CREATED---on_start,on_run-->RUNNING---on_finish,on_stop-->STOPPED
                                    |     ^               |         ^
                               on_wait on_resume,on_run   |   on_abort,on_stop
                                    v     |               |         |
                                    WAITING----------------     [any state]

        [any state]---on_fail-->FAILED

    ::

    When a Process enters a state is always gets a corresponding message, e.g.
    on entering RUNNING it will receive the on_run message.  These are
    always called immediately after that state is entered but before being
    executed.
    """

    # Static class stuff ######################
    _spec_type = ProcessSpec

    @classmethod
    def spec(cls):
        try:
            return cls.__getattribute__(cls, '_spec')
        except AttributeError:
            cls._spec = cls._spec_type()
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
    def define(cls, spec):
        cls.__called = True

    @classmethod
    def get_description(cls):
        """
        Get a human readable description of what this :class:`Process` does.

        :return: The description.
        :rtype: str
        """
        desc = []
        if cls.__doc__:
            desc.append("Description")
            desc.append("===========")
            desc.append(cls.__doc__)

        spec_desc = cls.spec().get_description()
        if spec_desc:
            desc.append("Specification")
            desc.append("=============")
            desc.append(spec_desc)

        return "\n".join(desc)

    def __init__(self, inputs=None, pid=None, logger=None):
        """
        The signature of the constructor should not be changed by subclassing
        processes.

        :param inputs: A dictionary of the process inputs
        :type inputs: dict
        :param pid: The process ID, if not a unique pid will be chosen
        :param logger: An optional logger for the process to use
        :type logger: :class:`logging.Logger`
        """
        super(Process, self).__init__()

        # Don't allow the spec to be changed anymore
        self.spec().seal()

        # Setup runtime state
        self.__init(logger)

        # Input/output
        self._check_inputs(inputs)
        self._raw_inputs = None if inputs is None else utils.AttributesFrozendict(inputs)
        self._parsed_inputs = utils.AttributesFrozendict(self.create_input_args(self.raw_inputs))
        self._outputs = {}

        # Set up a process ID
        if pid is None:
            self._pid = self.uuid
        else:
            self._pid = pid

        # State stuff
        self.__CREATION_TIME = time.time()
        self.__state = None
        self.__next_step = None
        self.__awaiting = None
        self.__loop_callback = None
        self.__paused = False
        self.__callback_fn = None
        self.__callback_args = None
        self.__abort_msg = None

    @property
    def creation_time(self):
        """
        The creation time of this Process as returned by time.time() when instantiated
        :return: The creation time
        :rtype: float
        """
        return self.__CREATION_TIME

    @property
    def pid(self):
        return self._pid

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
    def state(self):
        return self.__state

    @property
    def logger(self):
        """
        Get the logger for this class.  Can be None.

        :return: The logger.
        :rtype: :class:`logging.Logger`
        """
        if self.__logger is not None:
            return self.__logger
        else:
            return _LOGGER

    def has_finished(self):
        """
        Has the process finished i.e. completed running normally, without abort
        or an exception.

        :return: True if finished, False otherwise
        :rtype: bool
        """
        return self.has_terminated() and not self.has_aborted() and not self.has_failed()

    def has_failed(self):
        """
        Has the process failed i.e. an exception was raised

        :return: True if an unhandled exception was raised, False otherwise
        :rtype: bool
        """
        return self.has_terminated() and self.exception() is not None

    def has_terminated(self):
        """
        Is the process done

        :return: True if the process is STOPPED or FAILED, False otherwise
        :rtype: bool
        """
        return self.done()

    def has_aborted(self):
        return self.cancelled()

    def get_abort_msg(self):
        return self.__abort_msg

    def get_waiting_on(self):
        """
        Get the awaitable this process is waiting on, or None.

        :return: The awaitable or None
        :rtype: :class:`apricotpy.Awaitable` or None
        """
        return self.__awaiting

    def save_instance_state(self, out_state):
        """
        Ask the process to save its current instance state.

        :param out_state: A bundle to save the state to
        :type out_state: :class:`apricotpy.Bundle`
        """
        super(Process, self).save_instance_state(out_state)
        # Immutables first
        out_state[BundleKeys.CREATION_TIME] = self.creation_time
        out_state[BundleKeys.PID] = self.pid

        # Inputs/outputs
        if self.raw_inputs is not None:
            out_state[BundleKeys.INPUTS] = self.encode_input_args(self.raw_inputs)
        out_state[BundleKeys.OUTPUTS] = self._outputs

        # Now state stuff
        if self.__state is None:
            out_state[BundleKeys.PROC_STATE] = None
        else:
            out_state[BundleKeys.PROC_STATE] = self.__state.value
        if self.__next_step is not None:
            out_state[BundleKeys.NEXT_STEP] = self.__next_step.__name__

        out_state[BundleKeys.AWAITING] = self.__awaiting
        out_state[BundleKeys.LOOP_CALLBACK] = self.__loop_callback
        out_state[BundleKeys.PAUSED] = self.__paused
        out_state[BundleKeys.CALLBACK_FN] = self.__callback_fn
        out_state[BundleKeys.CALLBACK_ARGS] = self.__callback_args
        out_state[BundleKeys.ABORT_MSG] = self.__abort_msg

    @protected
    def load_instance_state(self, saved_state):
        super(Process, self).load_instance_state(saved_state)

        # Set up runtime state
        self.__init(None)

        # Inputs/outputs
        try:
            decoded = self.decode_input_args(saved_state[BundleKeys.INPUTS])
            self._raw_inputs = utils.AttributesFrozendict(decoded)
        except KeyError:
            self._raw_inputs = None

        self._parsed_inputs = utils.AttributesFrozendict(self.create_input_args(self.raw_inputs))
        self._outputs = copy.deepcopy(saved_state[BundleKeys.OUTPUTS])

        # Immutable stuff
        self.__CREATION_TIME = saved_state[BundleKeys.CREATION_TIME]
        self._pid = saved_state[BundleKeys.PID]

        # State stuff
        if saved_state[BundleKeys.PROC_STATE] is None:
            self.__state = None
        else:
            self.__state = ProcessState(saved_state[BundleKeys.PROC_STATE])
        try:
            self.__next_step = getattr(self, saved_state[BundleKeys.NEXT_STEP])
        except KeyError:
            self.__next_step = None

        self.__awaiting = saved_state[BundleKeys.AWAITING]
        self.__loop_callback = saved_state[BundleKeys.LOOP_CALLBACK]
        self.__paused = saved_state[BundleKeys.PAUSED]
        self.__callback_fn = saved_state[BundleKeys.CALLBACK_FN]
        self.__callback_args = saved_state[BundleKeys.CALLBACK_ARGS]
        self.__abort_msg = saved_state[BundleKeys.ABORT_MSG]

    def on_loop_inserted(self, loop):
        super(Process, self).on_loop_inserted(loop)
        self._do(self._enter_created)

    def abort(self, msg=None):
        """
        Abort the process.  Can optionally provide a message with
        the abort.  This can be called from another thread.

        :param msg: The abort message
        :type msg: str
        """
        self.log_with_pid(logging.INFO, "aborting")

        self._loop_check()
        self.play()

        if self.__loop_callback is not None:
            self.__loop_callback.cancel()

        fut = self.loop().create_future()
        self.loop().call_soon(self._do_abort, fut, msg)
        return fut

    def _do_abort(self, fut, msg=None):
        if not self.has_terminated():
            self._enter_stopped(abort=True, abort_msg=msg)

        fut.set_result(self.has_aborted())

    def play(self):
        if self.is_playing():
            return

        self.__paused = False
        if self._callback_fn is not None:
            self._schedule_callback(self._callback_fn, *self._callback_args)

    def pause(self):
        if not self.is_playing():
            return

        if self.__loop_callback is not None:
            self.__loop_callback.cancel()

        self.__paused = True

    def is_playing(self):
        return not self.__paused

    def add_process_listener(self, listener):
        assert (listener != self), "Cannot listen to yourself!"
        self.__event_helper.add_listener(listener)

    def remove_process_listener(self, listener):
        self.__event_helper.remove_listener(listener)

    def listen_scope(self, listener):
        return ListenContext(self, listener)

    @protected
    def set_logger(self, logger):
        self.__logger = logger

    @protected
    def log_with_pid(self, level, msg):
        self.logger.log(level, "{}: {}".format(self.pid, msg))

    # region Process messages
    # Make sure to call the superclass method if your override any of these
    @protected
    def on_create(self):
        """
        Called when the process is created.
        """
        # In this case there is no message fired because no one could have
        # registered themselves as a listener by this point in the lifecycle.

        self.__called = True

    @protected
    def on_start(self):
        """
        Called when this process is about to run for the first time.


        Any class overriding this method should make sure to call the super
        method, usually at the end of the function.
        """
        self._fire_event(ProcessListener.on_process_start)
        self._send_message('start')

        self.__called = True

    @protected
    def on_run(self):
        """
        Called when the process is about to run some code either for the first
        time (in which case an on_start message would have been received) or
        after something it was waiting on has finished (in which case an
        on_continue message would have been received).

        Any class overriding this method should make sure to call the super
        method.
        """
        self._fire_event(ProcessListener.on_process_run)
        self._send_message('run')

        self.__called = True

    @protected
    def on_wait(self, awaiting_uuid):
        """
        Called when the process is about to enter the WAITING state
        """
        self._fire_event(ProcessListener.on_process_wait)
        self._send_message('wait', {'awaiting': awaiting_uuid})

        self.__called = True

    @protected
    def on_resume(self):
        self._fire_event(ProcessListener.on_process_resume)
        self._send_message('resume')

        self.__called = True

    @protected
    def on_abort(self, abort_msg):
        """
        Called when the process has been asked to abort itself.
        """
        self.__abort_msg = abort_msg

        self._fire_event(ProcessListener.on_process_abort)
        self._send_message('abort', {'msg': abort_msg})

        self.__called = True

    @protected
    def on_finish(self):
        """
        Called when the process has finished and the outputs have passed
        checks
        """
        self._check_outputs()
        self._fire_event(ProcessListener.on_process_finish)
        self._send_message('finish')

        self.__called = True

    @protected
    def on_stop(self):
        self._fire_event(ProcessListener.on_process_stop)
        self._send_message('stop')

        self.__called = True

    @protected
    def on_fail(self, exc_info):
        """
        Called if the process raised an exception.

        :param exc_info: The exception information as returned by sys.exc_info()
        """
        self._fire_event(ProcessListener.on_process_fail)
        self._send_message('fail')

        self.__called = True

    @protected
    def on_terminate(self):
        """
        Called when the process reaches a terminal state.
        """
        self._fire_event(ProcessListener.on_process_terminate)
        self._send_message('terminate')

        self.__called = True

    def on_output_emitted(self, output_port, value, dynamic):
        self.__event_helper.fire_event(ProcessListener.on_output_emitted,
                                       self, output_port, value, dynamic)

    # endregion

    @protected
    def do_run(self):
        return self._run(**(self.inputs if self.inputs is not None else {}))

    @protected
    def out(self, output_port, value):
        dynamic = False
        # Do checks on the outputs
        try:
            # Check types (if known)
            port = self.spec().get_output(output_port)
        except KeyError:
            if self.spec().has_dynamic_output():
                dynamic = True
                port = self.spec().get_dynamic_output()
            else:
                raise TypeError(
                    "Process trying to output on unknown output port {}, "
                    "and does not have a dynamic output port in spec.".
                        format(output_port))

            if port.valid_type is not None and not isinstance(value, port.valid_type):
                raise TypeError(
                    "Process returned output '{}' of wrong type."
                    "Expected '{}', got '{}'".
                        format(output_port, port.valid_type, type(value)))

        self._outputs[output_port] = value
        self.on_output_emitted(output_port, value, dynamic)

    @protected
    def create_input_args(self, inputs):
        """
        Take the passed input arguments and fill in any default values for
        inputs that have no been supplied.

        Preconditions:
        * All required inputs have been supplied

        :param inputs: The supplied input values.
        :return: A dictionary of inputs including any with default values
        """
        if inputs is None:
            ins = {}
        else:
            ins = dict(inputs)
        # Go through the spec filling in any default and checking for required
        # inputs
        for name, port in self.spec().inputs.items():
            if name not in ins:
                if port.default != _NULL:
                    ins[name] = port.default
                elif port.required:
                    raise ValueError(
                        "Value not supplied for required inputs port {}".format(name)
                    )

        return ins

    @protected
    def encode_input_args(self, inputs):
        """
        Encode input arguments such that they may be saved in a
        :class:`apricotpy.persistable.Bundle`

        :param inputs: A mapping of the inputs as passed to the process
        :return: The encoded inputs
        """
        return inputs

    @protected
    def decode_input_args(self, encoded):
        """
        Decode saved input arguments as they came from the saved instance state
        :class:`apricotpy.persistable.Bundle`

        :param encoded:
        :return: The decoded input args
        """
        return encoded

    def __init(self, logger):
        """
        Common place to put all runtime state variables i.e. those that don't need
        to be persisted.  This can be called from the constructor or
        load_instance_state.
        """
        self.__logger = logger

        # Events and running
        self.__event_helper = utils.EventHelper(ProcessListener)

        # Flag to make sure all the necessary event methods were called
        self.__called = False

    # region State event/transition methods

    def _fire_event(self, event):
        self.loop().call_soon(self.__event_helper.fire_event, event, self)

    def _send_message(self, subject, body_=None):
        body = {'uuid': self.uuid}
        if body_ is not None:
            body.update(body_)
        self.send_message('process.{}.{}'.format(self.pid, subject), body)

    def _terminate(self):
        self._call_with_super_check(self.on_terminate)

    def _call_with_super_check(self, fn, *args, **kwargs):
        """
        Call one of our state event methods making sure super() was called
        by the subclassing class.
        """
        self.__called = False
        fn(*args, **kwargs)
        assert self.__called, \
            "{} was not called\n" \
            "Hint: Did you forget to call the superclass method?".format(fn.__name__)

    # endregion

    def _check_inputs(self, inputs):
        # Check the inputs meet the requirements
        valid, msg = self.spec().validate(inputs)
        if not valid:
            raise ValueError(msg)

    def _check_outputs(self):
        # Check that the necessary outputs have been emitted
        for name, port in self.spec().outputs.items():
            valid, msg = port.validate(self._outputs.get(name, None))
            if not valid:
                raise RuntimeError(
                    "Process {} failed because {}".format(self.get_name(), msg)
                )

    def _loop_check(self):
        assert self.in_loop(), "The process is not in the event loop"

    @abstractmethod
    def _run(self, **kwargs):
        pass

    def _enter_created(self):
        self.__state = ProcessState.CREATED
        self._call_with_super_check(self.on_create)
        self._schedule_callback(self._exec_created)

    def _exec_created(self):
        self._enter_running(self.do_run)

    def _enter_running(self, next_step, result=None):
        last_state = self.__state
        self._set_state(ProcessState.RUNNING,
                        [ProcessState.CREATED, ProcessState.WAITING, ProcessState.RUNNING])

        if last_state is ProcessState.CREATED:
            self._call_with_super_check(self.on_start)
        elif last_state is ProcessState.WAITING:
            self._call_with_super_check(self.on_resume)

        self.__next_step = next_step
        self._last_result = result
        self._call_with_super_check(self.on_run)
        self._schedule_callback(self._exec_running, next_step, result)

    def _exec_running(self, next_step, result):
        args = []
        if _should_pass_result(next_step):
            args.append(result)

        # Run the next function
        try:
            try:
                _stack.push(self)
                retval = next_step(*args)
            finally:
                _stack.pop(self)

        except BaseException:
            self._enter_failed(sys.exc_info())
        else:
            if _is_wait_retval(retval):
                awaitable, callback = retval
                self._enter_waiting(awaitable, callback)
            else:
                self._enter_stopped()

    def _enter_waiting(self, awaiting, next_step):
        self.__state = ProcessState.WAITING
        self.__awaiting = awaiting
        self.__next_step = next_step
        self._call_with_super_check(self.on_wait, awaiting)
        # There's no exec_waiting() because all is has to do is wait for the
        # thing that it's awaiting
        awaiting.add_done_callback(apricotpy.persistable.Function(self._do, self._await_done))

    def _await_done(self, awaitable):
        self.__awaiting = None

        if self.__next_step is None:
            self._enter_stopped()
        else:
            self._enter_running(self.__next_step, awaitable.result())

    def _enter_stopped(self, abort=False, abort_msg=None):
        last_state = self.__state
        if not abort and last_state not in [ProcessState.RUNNING, ProcessState.WAITING]:
            raise RuntimeError("Cannot enter STOPPED state from {}".format(self.__state))

        self._set_state(ProcessState.STOPPED)
        if abort:
            self._call_with_super_check(self.on_abort, abort_msg)
        elif last_state in [ProcessState.RUNNING, ProcessState.WAITING]:
            self._call_with_super_check(self.on_finish)

        self._call_with_super_check(self.on_stop)
        if abort:
            self._exec_stopped(abort)
        else:
            self._schedule_callback(self._exec_stopped, abort)

    def _exec_stopped(self, abort):
        self._terminate()
        if abort:
            self.cancel()
        else:
            self.set_result(self.outputs)

    def _enter_failed(self, exc_info):
        self._set_state(ProcessState.FAILED)
        try:
            self._call_with_super_check(self.on_fail, exc_info)
        except BaseException:
            import traceback
            self.log_with_pid(
                logging.ERROR, "exception entering failed state:\n{}".format(traceback.format_exc()))

        self._schedule_callback(self._exc_failed, exc_info[1])

    def _exc_failed(self, exception):
        self._terminate()
        self.set_exception(exception)

    def _set_state(self, new_state, allowed_states=None):
        """
        Set the state optionally checking that we are entering from an allowed state.

        :param new_state: The new state
        :param allowed_states: An optional tuple of states we are allowed to enter
            this state from.
        """
        if allowed_states is not None and self.__state not in allowed_states:
            raise RuntimeError(
                "Cannot enter state {} from {}".format(new_state, self.__state)
            )
        self.__state = new_state

    def _schedule_callback(self, fn, *args):
        assert inspect.ismethod(fn) and fn.__self__ is self, \
            "Callback has to be a member of this process"

        self._callback_fn = fn
        self._callback_args = args
        # If not playing, then the play call will schedule the callback
        if self.is_playing():
            self.__loop_callback = self.loop().call_soon(self._do, fn, *args)

    def _do(self, fn, *args):
        try:
            self.__loop_callback = None
            self._callback_args = None
            self._callback_fn = None
            fn(*args)
        except BaseException:
            self._enter_failed(sys.exc_info())


class ListenContext(object):
    """
    A context manager for listening to the Process.

    A typical usage would be:
    with ListenContext(producer, listener):
        # Producer generates messages that the listener gets
        pass
    """

    def __init__(self, producer, *args, **kwargs):
        self._producer = producer
        self._args = args
        self._kwargs = kwargs

    def __enter__(self):
        self._producer.add_process_listener(*self._args, **self._kwargs)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._producer.remove_process_listener(*self._args, **self._kwargs)


def _is_wait_retval(retval):
    """
    Determine if the value provided is a valid Wait return value which consists
    of a 2-tuple of a WaitOn and a callback function (or None) to be called
    after the wait on is ready

    :param retval: The return value from a step to check
    :return: True if it is a valid wait object, False otherwise
    """
    return (isinstance(retval, tuple) and
            len(retval) == 2 and
            isinstance(retval[0], apricotpy.Awaitable))


def get_pid_from_bundle(process_bundle):
    return process_bundle[BundleKeys.PID]
