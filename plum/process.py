# -*- coding: utf-8 -*-

import logging
import sys
import time
from abc import ABCMeta, abstractmethod
from collections import namedtuple

import plum.loop.persistence
import plum.stack as _stack
from plum.persistence.bundle import Bundle
from plum.process_listener import ProcessListener
from plum.process_monitor import MONITOR
from plum.process_spec import ProcessSpec
from plum.process_states import *
from plum.util import protected, get_default_loop
from plum.wait import WaitOn
from plum.loop import tasks

__all__ = ['Process']

_LOGGER = logging.getLogger(__name__)

Wait = namedtuple('Wait', ['on', 'callback'])


class Process(plum.loop.persistence.PersistableTask):
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
    __metaclass__ = ABCMeta

    # Static class stuff ######################
    _spec_type = ProcessSpec

    class BundleKeys(Enum):
        """
        String keys used by the process to save its state in the state bundle.

        See :func:`create_from`, :func:`save_instance_state` and :func:`load_instance_state`.
        """
        CREATION_TIME = 'creation_time'
        CLASS_NAME = 'class_name'
        INPUTS = 'inputs'
        OUTPUTS = 'outputs'
        PID = 'pid'
        STATE = 'state'
        FINISHED = 'finished'
        TERMINATED = 'terminated'
        WAIT_ON = 'wait_on'

    @staticmethod
    def _is_wait_retval(retval):
        """
        Determine if the value provided is a valid Wait retval which consists
        of a 2-tuple of a WaitOn and a callback function (or None) to be called
        after the wait on is ready

        :param retval: The return value from a step to check
        :return: True if it is a valid wait object, False otherwise
        """
        return (isinstance(retval, tuple) and
                len(retval) == 2 and
                isinstance(retval[0], WaitOn))

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

    def __init__(self, loop, inputs=None, pid=None, logger=None):
        """
        The signature of the constructor should not be changed by subclassing
        processes.

        :param inputs: A dictionary of the process inputs
        :type inputs: dict
        :param pid: The process ID, if not a unique pid will be chosen
        :param logger: An optional logger for the process to use
        :type logger: :class:`logging.Logger`
        """
        super(Process, self).__init__(loop)

        # Don't allow the spec to be changed anymore
        self.spec().seal()

        # Setup runtime state
        self.__init(logger)

        # Input/output
        self._check_inputs(inputs)
        self._raw_inputs = None if inputs is None else util.AttributesFrozendict(inputs)
        self._parsed_inputs = util.AttributesFrozendict(self.create_input_args(self.raw_inputs))
        self._outputs = {}

        # Set up a process ID
        if pid is None:
            self._pid = self.uuid
        else:
            self._pid = pid

        # State stuff
        self._CREATION_TIME = time.time()
        self._finished = False
        self._terminated = False
        self._state_bundle = None

        # Finally enter the CREATED state
        self._state = Created(self)
        self._state.enter(None)

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
        return self._state.label

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
        return self._finished

    def has_failed(self):
        """
        Has the process failed i.e. an exception was raised

        :return: True if an unhandled exception was raised, False otherwise
        :rtype: bool
        """
        return self.get_exc_info() is not None

    def has_terminated(self):
        """
        Has the process terminated

        :return: True if the process is STOPPED or FAILED, False otherwise
        :rtype: bool
        """
        return self._terminated

    def has_aborted(self):
        try:
            return self._state.get_aborted()
        except AttributeError:
            return False

    def get_abort_msg(self):
        try:
            return self._state.get_abort_msg()
        except AttributeError:
            return None

    def get_waiting_on(self):
        """
        Get the awaitable this process is waiting on, or None.

        :return: The awaitable or None
        :rtype: :class:`plum.loop.object.Awaitable`
        """
        return self.awaiting()

    def get_exception(self):
        exc_info = self.get_exc_info()
        if exc_info is None:
            return None

        return exc_info[1]

    def get_exc_info(self):
        """
        If this process produced an exception that caused it to fail during its
        execution then it will have store the execution information as obtained
        from sys.exc_info(), this method returns it.  If there was no exception
        then None is returned.

        :return: The exception info if process failed, None otherwise
        """
        try:
            return self._state.get_exc_info()
        except AttributeError:
            return None

    def save_instance_state(self, bundle):
        """
        Ask the process to save its current instance state.

        :param bundle: A bundle to save the state to
        :type bundle: :class:`plum.persistence.Bundle`
        """
        super(Process, self).save_instance_state(bundle)
        # Immutables first
        bundle[self.BundleKeys.CREATION_TIME.value] = self.creation_time
        bundle[self.BundleKeys.CLASS_NAME.value] = util.fullname(self)
        bundle[self.BundleKeys.PID.value] = self.pid

        # Now state stuff
        state_bundle = Bundle()
        self._state.save_instance_state(state_bundle)
        bundle[self.BundleKeys.STATE.value] = state_bundle

        bundle[self.BundleKeys.FINISHED.value] = self._finished
        bundle[self.BundleKeys.TERMINATED.value] = self._terminated

        # Inputs/outputs
        bundle.set_if_not_none(self.BundleKeys.INPUTS.value, self.raw_inputs)
        bundle[self.BundleKeys.OUTPUTS.value] = Bundle(self._outputs)

    def execute(self):
        return self._tick()

    def _tick(self, wait_on=None):
        try:
            _stack.push(self)
            MONITOR.register_process(self)

            if self.__aborting:
                self._set_state(Stopped(self, abort=True, abort_msg=self.__abort_msg))
                self.__aborting, self.__abort_msg = False, None

            # Perform the actions of this state
            result = self._state.execute()

            if isinstance(result, Awaitable):
                return tasks.Await(self._tick, result)
            elif self.has_terminated():
                return self.outputs
            else:
                return tasks.Continue(self._tick)

        except BaseException:
            self._set_state(Failed(self, sys.exc_info()))
            raise

        finally:
            MONITOR.deregister_process(self)
            _stack.pop(self)

    def run(self):
        """
        Run the process until it is finished.
        """
        if self.has_terminated():
            raise RuntimeError("Cannot run, already terminated")

        return self.loop().run_until_complete(self)

    def abort(self, msg=None):
        """
        Abort a playing process.  Can optionally provide a message with
        the abort.  This can be called from another thread.

        :param msg: The abort message
        :type msg: str
        """
        self.log_with_pid(logging.INFO, "aborting")
        if self.has_terminated():
            return False

        self.__aborting = True
        self.__abort_msg = msg
        if super(Process, self).cancel():
            # Tick ourselves to make the transition to the ABORTED state
            self._tick()

        return True

    def cancel(self):
        return self.abort(msg="Task cancelled")

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

    def on_loop_inserted(self, loop):
        super(Process, self).on_loop_inserted(loop)

        # Load the state, needed to wait till here to do it because at the time
        # of load_instance_state we don't have the loop yet which may be needed
        # by the state
        if self._state_bundle is not None:
            self._state = load_state(self, self._state_bundle)
            self._state_bundle = None

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
    def on_wait(self, wait_on):
        """
        Called when the process is about to enter the WAITING state
        """
        self._fire_event(ProcessListener.on_process_wait)
        self._send_message('wait', wait_on)

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
        self._fire_event(ProcessListener.on_process_abort)
        self._send_message('abort', abort_msg)
        if not self.cancelled():
            super(Process, self).cancel()

        self.__called = True

    @protected
    def on_finish(self):
        """
        Called when the process has finished and the outputs have passed
        checks
        """
        self._check_outputs()
        self._finished = True
        self._fire_event(ProcessListener.on_process_finish)
        self._send_message('finish')

        self.__called = True

    @protected
    def on_stop(self):
        self._fire_event(ProcessListener.on_process_stop)
        self._send_message('stop')

        self.__called = True

    @protected
    def on_fail(self):
        """
        Called if the process raised an exception.
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
    def load_instance_state(self, loop, saved_state, logger=None):
        super(Process, self).load_instance_state(loop, saved_state)
        self.__init(logger)

        # Immutable stuff
        self._CREATION_TIME = saved_state[self.BundleKeys.CREATION_TIME.value]
        self._pid = saved_state[self.BundleKeys.PID.value]

        # State stuff
        self._finished = saved_state[self.BundleKeys.FINISHED.value]
        self._terminated = saved_state[self.BundleKeys.TERMINATED.value]
        self._state_bundle = saved_state[self.BundleKeys.STATE.value]

        # Inputs/outputs
        try:
            self._raw_inputs = util.AttributesFrozendict(saved_state[self.BundleKeys.INPUTS.value])
        except KeyError:
            self._raw_inputs = None
        self._parsed_inputs = util.AttributesFrozendict(self.create_input_args(self.raw_inputs))
        self._outputs = saved_state[self.BundleKeys.OUTPUTS.value].get_dict_deepcopy()

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
        for name, port in self.spec().inputs.iteritems():
            if name not in ins:
                if port.default:
                    ins[name] = port.default
                elif port.required:
                    raise ValueError(
                        "Value not supplied for required inputs port {}".format(name)
                    )

        return ins

    def __init(self, logger):
        """
        Common place to put all runtime state variables i.e. those that don't need
        to be persisted.  This can be called from the constructor or
        load_instance_state.
        """
        self.__logger = logger
        self.__aborting = None
        self.__abort_msg = None

        # Events and running
        self.__event_helper = util.EventHelper(ProcessListener)

        # Flag to make sure all the necessary event methods were called
        self.__called = False

    # region State event/transition methods

    def _on_start_playing(self):
        _stack.push(self)
        MONITOR.register_process(self)

    def _on_stop_playing(self):
        """
        WARNING: No state changes should be made after this call.
        """
        MONITOR.deregister_process(self)
        _stack.pop(self)

        if self.has_terminated():
            # There will be no more messages so remove the listeners.  Otherwise we
            # may continue to hold references to them and stop them being garbage
            # collected
            self.__event_helper.remove_all_listeners()

    def _on_create(self):
        self._call_with_super_check(self.on_create)

    def _on_start(self):
        self._call_with_super_check(self.on_start)

    def _on_resume(self):
        self._call_with_super_check(self.on_resume)

    def _on_run(self):
        self._call_with_super_check(self.on_run)

    def _on_wait(self, wait_on):
        self._call_with_super_check(self.on_wait, wait_on)

    def _on_finish(self):
        self._call_with_super_check(self.on_finish)

    def _on_abort(self, msg):
        self._call_with_super_check(self.on_abort, msg)

    def _on_stop(self, msg):
        self._call_with_super_check(self.on_stop)

    def _on_fail(self, exc_info):
        self._call_with_super_check(self.on_fail)

    def _fire_event(self, event):
        self.loop().call_soon(self.__event_helper.fire_event, event, self)

    def _send_message(self, subject, body=None):
        self.loop().messages().send('process.{}.{}'.format(self.pid, subject), body)

    def _terminate(self):
        self._terminated = True
        self._call_with_super_check(self.on_terminate)

    def _call_with_super_check(self, fn, *args, **kwargs):
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
        for name, port in self.spec().outputs.iteritems():
            valid, msg = port.validate(self._outputs.get(name, None))
            if not valid:
                raise RuntimeError("Process {} failed because {}".
                                   format(self.get_name(), msg))

    def _set_state(self, state):
        previous_state = self._state.label
        self._state.exit()
        self._state = state
        self._state.enter(previous_state)

    @abstractmethod
    def _run(self, **kwargs):
        pass


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
