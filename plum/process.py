# -*- coding: utf-8 -*-

import uuid
from enum import Enum
from abc import ABCMeta, abstractmethod
from collections import namedtuple
import threading

import plum.knowledge_provider as knowledge_provider
import plum.error as error
from plum.wait import Interrupted
from plum.persistence.bundle import Bundle
from plum.process_listener import ProcessListener
from plum.process_monitor import MONITOR
from plum.process_spec import ProcessSpec
from plum.util import protected
import plum.util as util
from plum.wait import WaitOn
from plum._base import LOGGER


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


class _Unlock(object):
    def __init__(self, lock):
        """
        :param lock: :class:`threading.Lock`
        """
        self._lock = lock

    def __enter__(self):
        self._lock.release()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.acquire()


class Process(object):
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

        CREATED---on_start,on_run-->RUNNING---on_finish,on_stop-->STOPPED
                                    |     ^               |         ^
                               on_wait  on_resume,on_run  |   on_abort,on_stop
                                    v     |               |         |
                                    WAITING----------------     [any state]

        [any state]---on_fail-->FAILED

    ::

    When a Process enters a state is always gets a corresponding message, e.g.
    on entering RUNNING it will receive the on_run message.  These are
    always called immediately after that state is entered.

    """
    __metaclass__ = ABCMeta

    # Static class stuff ######################
    _spec_type = ProcessSpec

    class BundleKeys(Enum):
        """
        String keys used by the process to save its state in the state bundle.

        See :func:`create_from`, :func:`save_instance_state` and :func:`load_instance_state`.
        """
        CLASS_NAME = 'class_name'
        INPUTS = 'inputs'
        OUTPUTS = 'outputs'
        PID = 'pid'
        WAITING_ON = 'waiting_on'
        WAIT_ON_CALLBACK = 'wait_on_callback'
        STATE = 'state'
        FINISHED = 'finished'
        ABORTED = 'aborted'
        ABORT_MSG = 'abort_msg'
        EXCEPTION = 'exception'
        NEXT_TRANSITION = 'next_transition'

    @staticmethod
    def instantiate(ProcClass, logger=None):
        p = ProcClass()
        if logger:
            p.set_logger(logger)
        else:
            p.set_logger(LOGGER)
        return p

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
    def new_instance(cls, inputs=None, pid=None, logger=None):
        """
        Create a new instance of this Process class with the given inputs and
        pid.

        :param inputs: The inputs for the newly created :class:`Process`
            instance.  Any mapping type is valid
        :param pid: The process id given to the new process.
        :param logger: The logger for this process to use, can be None.
        :type logger: :class:`logging.Logger`
        :return: An instance of this :class:`Process`.
        """
        proc = Process.instantiate(cls, logger)
        proc._perform_create(pid, inputs)
        return proc

    @classmethod
    def create_from(cls, saved_instance_state, logger=None):
        """
        Create a process from a saved instance state.

        :param saved_instance_state: The saved state
        :type saved_instance_state: Bundle
        :param logger: The logger for this process to use
        :return: An instance of this process with its state loaded from the save state.
        :rtype: :class:`Process`
        """
        # Get the class using the class loader and instantiate it
        class_name = saved_instance_state[cls.BundleKeys.CLASS_NAME.value]
        ProcClass = \
            saved_instance_state.get_class_loader().load_class(class_name)
        proc = Process.instantiate(ProcClass, logger)
        # Get it to create itself
        proc._perform_create(saved_instance_state=saved_instance_state)

        return proc

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
            desc.append("Specifications")
            desc.append("==============")
            desc.append(spec_desc)

        return "\n".join(desc)

    @classmethod
    def run(cls, **kwargs):
        p = cls.new_instance(kwargs)
        p.play()
        return p.outputs

    @classmethod
    def define(cls, spec):
        cls.__called = True

    ############################################

    def __init__(self):
        # Don't allow the spec to be changed anymore
        self.spec().seal()

        self._logger = None

        self._pid = None
        # State stuff
        self._state = None
        self._finished = False
        self._exception = None
        self._wait = None
        self._next_transition = None
        self._aborted = False
        self._abort_msg = None

        # Input/output
        self._raw_inputs = None
        self._parsed_inputs = None
        self._outputs = {}

        # RUNTIME STATE ##
        # Stuff below here doesn't need to be saved in the instance state
        # Reads/writes of variables with 'protect' suffix should be guarded by
        # the state lock
        self.__pausing_protect = False
        self.__aborting_protect = False
        self.__playing = False
        self.__state_lock = threading.Lock()

        # Events and running
        self.__event_helper = util.EventHelper(ProcessListener)

        # Flag to make sure all the necessary event methods were called
        self.__called = False

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
        return self._state

    @property
    def logger(self):
        """
        Get the logger for this class.  Can be None.

        :return: The logger.
        :rtype: :class:`logging.Logger`
        """
        return self._logger

    def is_playing(self):
        """
        Is the process currently playing.

        :return: True if playing, False otherwise
        :rtype: bool
        """
        return self.__playing

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
        return self._exception is not None

    def has_terminated(self):
        """
        Has the process terminated

        :return: True if has_finished() or has_failed(), False otherwise
        :rtype: bool
        """
        return self.has_finished() or self.has_failed()

    def has_aborted(self):
        return self._aborted

    def get_waiting_on(self):
        """
        Get the WaitOn this process is waiting on, or None.

        :return: The WaitOn or None
        :rtype: :class:`plum.wait.WaitOn`
        """
        try:
            return self._wait.on
        except AttributeError:
            return None

    def get_exception(self):
        return self._exception

    def save_instance_state(self, bundle):
        bundle[self.BundleKeys.CLASS_NAME.value] = util.fullname(self)
        bundle[self.BundleKeys.STATE.value] = self.state
        bundle[self.BundleKeys.PID.value] = self.pid
        bundle[self.BundleKeys.FINISHED.value] = self._finished
        bundle[self.BundleKeys.ABORTED.value] = self._aborted
        bundle.set_if_not_none(self.BundleKeys.EXCEPTION.value, self._exception)
        if self._next_transition is not None:
            bundle[self.BundleKeys.NEXT_TRANSITION.value] = \
                self._next_transition.__name__

        bundle.set_if_not_none(self.BundleKeys.ABORT_MSG.value, self._abort_msg)

        # Save inputs and outputs
        bundle.set_if_not_none(self.BundleKeys.INPUTS.value, self.raw_inputs)
        bundle[self.BundleKeys.OUTPUTS.value] = Bundle(self._outputs)

        if self._wait is not None:
            b = Bundle()
            self.get_waiting_on().save_instance_state(b)
            bundle[self.BundleKeys.WAITING_ON.value] = b
            bundle[self.BundleKeys.WAIT_ON_CALLBACK.value] = \
                self._wait.callback.__name__

    def play(self):
        """
        Play the process.
        """
        assert not self.__playing, \
            "Cannot execute a process twice simultaneously"

        try:
            MONITOR.register_process(self)
            with self.__state_lock:
                self._call_with_super_check(self.on_playing)

            # Keep going until we run out of tasks
            fn = self._next()
            while fn is not None:
                try:
                    with self.__state_lock:
                        self._next_transition = None
                        fn()
                except BaseException as e:
                    self._perform_fail(e)
                    raise
                fn = self._next()

        finally:
            MONITOR.deregister_process(self)
            self._call_with_super_check(self.on_done_playing)

        return self._outputs

    def pause(self):
        """
        Pause an playing process.  This can be called from another thread.
        """
        with self.__state_lock:
            self.__pausing_protect = True
            self._interrupt()

    def abort(self, msg=None):
        """
        Abort a playing process.  Can optionally provide a message with
        the abort.  This can be called from another thread.

        :param msg: The abort message
        :type msg: str
        """
        with self.__state_lock:
            self.__aborting_protect = True
            self._abort_msg = msg
            self._interrupt()

    def add_process_listener(self, listener):
        assert (listener != self)
        self.__event_helper.add_listener(listener)

    def remove_process_listener(self, listener):
        self.__event_helper.remove_listener(listener)

    @protected
    def set_logger(self, logger):
        self._logger = logger

    # Process messages #####################################################
    # Make sure to call the superclass method if your override any of these
    @protected
    def on_playing(self):
        self.__playing = True
        self.__pausing_protect = False
        self.__aborting_protect = False

        self.__called = True

    @protected
    def on_done_playing(self):
        self.__playing = False

        self.__called = True

    @protected
    def on_create(self, pid, inputs, saved_instance_state):
        """
        Called when the process is created.  If a checkpoint is supplied the
        process should reinstate its state at the time the checkpoint was taken
        and if the checkpoint has a wait_on the process will continue from the
        corresponding callback function.

        :param inputs: The inputs the process should run using.
        """
        # In this case there is no message fired because no one could have
        # registered themselves as a listener by this point in the lifecycle.
        if saved_instance_state is not None:
            self.load_instance_state(saved_instance_state)
        else:
            if pid is None:
                pid = uuid.uuid1()
            self._pid = pid
            self._check_inputs(inputs)
            if inputs is not None:
                self._raw_inputs = util.AttributesFrozendict(inputs)
            self._parsed_inputs = \
                util.AttributesFrozendict(self.create_input_args(self.raw_inputs))

        self.__called = True

    @protected
    def on_start(self):
        """
        Called when the process is about to run for the first time.


        Any class overriding this method should make sure to call the super
        method, usually at the end of the function.

        """
        self.__event_helper.fire_event(ProcessListener.on_process_start, self)
        self.__called = True

    @protected
    def on_run(self):
        """
        Called when the process is about to run some code either for the first
        time (in which case an on_start message would have been received) or
        after something it was waiting on has finished (in which case an
        on_continue message would have been received).

        Any class overriding this method should make sure to call the super
        method, usually at the end of the function.

        """
        self._wait = None
        self.__event_helper.fire_event(ProcessListener.on_process_run, self)
        self.__called = True

    @protected
    def on_wait(self):
        self.__event_helper.fire_event(ProcessListener.on_process_wait, self)
        self.__called = True

    @protected
    def on_resume(self):
        self.__event_helper.fire_event(ProcessListener.on_process_resume, self)
        self.__called = True

    @protected
    def on_abort(self):
        """
        Called when the process has been asked to abort itself.
        """
        self._aborted = True
        self.__called = True

    @protected
    def on_finish(self):
        """
        Called when the process has finished and the outputs have passed
        checks
        """
        self._check_outputs()
        self._finished = True
        self.__event_helper.fire_event(ProcessListener.on_process_finish, self)
        self.__called = True

    @protected
    def on_stop(self):
        self.__event_helper.fire_event(ProcessListener.on_process_stop, self)
        # There will be no more messages so remove the listeners.  Otherwise we
        # may continue to hold references to them and stop them being garbage
        # collected
        self.__event_helper.remove_all_listeners()
        self.__called = True

    @protected
    def on_fail(self):
        """
        Called if the process raised an exception.
        """
        self.__event_helper.fire_event(ProcessListener.on_process_fail, self)
        # There will be no more messages so remove the listeners.  Otherwise we
        # may continue to hold references to them and stop them being garbage
        # collected
        self.__event_helper.remove_all_listeners()
        self.__called = True

    def _on_output_emitted(self, output_port, value, dynamic):
        self.__event_helper.fire_event(ProcessListener.on_output_emitted,
                                       self, output_port, value, dynamic)

    #####################################################################

    @protected
    def do_run(self):
        try:
            return self.fast_forward()
        except error.FastForwardError:
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

            if port.valid_type is not None and \
                    not isinstance(value, port.valid_type):
                raise TypeError(
                    "Process returned output '{}' of wrong type."
                    "Expected '{}', got '{}'".
                        format(output_port, port.valid_type, type(value)))

        self._outputs[output_port] = value
        self._on_output_emitted(output_port, value, dynamic)

    @protected
    def fast_forward(self):
        if not self.spec().is_deterministic():
            raise error.FastForwardError("Cannot fast-forward a process that "
                                         "is not deterministic")

        kp = knowledge_provider.get_global_provider()
        if kp is None:
            raise error.FastForwardError("Cannot fast-forward because a global"
                                         "knowledge provider is not available")

        # Try and find out if anyone else has had the same inputs
        try:
            pids = kp.get_pids_from_classname(util.fullname(self))
        except knowledge_provider.NotKnown:
            pass
        else:
            for pid in pids:
                try:
                    if kp.get_inputs(pid) == self.inputs:
                        for name, value in kp.get_outputs(pid).iteritems():
                            self.out(name, value)
                        return
                except knowledge_provider.NotKnown:
                    pass

        raise error.FastForwardError("Cannot fast forward")

    @protected
    def load_instance_state(self, bundle):
        self._pid = bundle[self.BundleKeys.PID.value]
        self._state = bundle[self.BundleKeys.STATE.value]
        self._finished = bundle[self.BundleKeys.FINISHED.value]
        self._aborted = bundle[self.BundleKeys.ABORTED.value]

        try:
            self._exception = bundle[self.BundleKeys.EXCEPTION.value]
        except KeyError:
            pass

        try:
            self._next_transition = self.__getattribute__(
                bundle[self.BundleKeys.NEXT_TRANSITION.value])
        except KeyError:
            pass

        try:
            self._abort_msg = bundle[self.BundleKeys.ABORT_MSG.value]
        except KeyError:
            pass

        try:
            b = bundle[self.BundleKeys.WAITING_ON.value]
            wait_on = WaitOn.create_from(b)
        except KeyError:
            pass  # There's no wait_on
        else:
            callback = self._get_wait_on_callback_fn(bundle)
            self._set_wait(wait_on, callback)

        # Get the inputs and outputs
        inputs = bundle.get(self.BundleKeys.INPUTS.value, None)
        if inputs is not None:
            self._raw_inputs = util.AttributesFrozendict(inputs)
        self._parsed_inputs = \
            util.AttributesFrozendict(self.create_input_args(self.raw_inputs))
        self._outputs = bundle[self.BundleKeys.OUTPUTS.value].get_dict_deepcopy()

    def _get_wait_on_callback_fn(self, bundle):
        """
        Get the callback function that should be called when the wait on has
        finished from the saved bundle.

        :param bundle: The bundle to get the function from
        :return: The function or None if it is not stored in the bundle
        """
        callback = None
        try:
            name = bundle[self.BundleKeys.WAIT_ON_CALLBACK.value]
        except KeyError:
            pass
        else:
            try:
                callback = self.__getattribute__(name)
            except AttributeError:
                raise ValueError(
                    "This process does not have a function with "
                    "the name '{}' as expected from the wait on".
                        format(name))
        return callback

    # Inputs ##################################################################
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
                        "Value not supplied for required inputs port {}".
                            format(name)
                    )

        return ins

    # State transition methods ################################################
    def _next(self):
        """
        Method to get the next method to run as part of the Process lifecycle.

        :return: A callable that only takes the self process as argument.
          May be None if the process should not continue.
        """
        with self.__state_lock:
            if self.has_terminated():
                return None
            elif self.__pausing_protect:
                return None
            elif self.__aborting_protect:
                return self._perform_abort
            else:
                return self._next_transition

    def _perform_create(self, pid=None, inputs=None, saved_instance_state=None):
        """

        :param pid: The process ID to use, can be None in which case one will
            be generated
        :param inputs: The process inputs
        :type inputs: dict
        :param saved_instance_state: An optional saved state to recreate from
        :type saved_instance_state: `class`:plum.persistence.bundle.Bundle`
        """
        self._state = ProcessState.CREATED
        self._call_with_super_check(self.on_create, pid, inputs, saved_instance_state)
        self._next_transition = self._perform_start

    def _perform_start(self):
        """
        Perform the state transition from CREATED -> RUNNING.
        Messages issued:
         - on_start
         - on_run
        """
        assert self.state is ProcessState.CREATED

        self._call_with_super_check(self.on_start)
        self._to_running()

        # Run the thing
        with _Unlock(self.__state_lock):
            self._return_value = self.do_run()
        # Figure out what to do next
        if self._is_wait_retval(self._return_value):
            self._set_wait(self._return_value[0], self._return_value[1])
            self._next_transition = self._perform_wait
        else:
            self._next_transition = self._perform_finish

    def _perform_wait(self):
        """
        Messages issued (if not already waiting):
         - on_wait
        """
        # This could get called when the process was already in the waiting
        # state just because it could be resuming from being paused or from
        # a saved state
        if self.state is not ProcessState.WAITING:
            assert self.state is ProcessState.RUNNING

            self._state = ProcessState.WAITING
            self._call_with_super_check(self.on_wait)

        try:
            with _Unlock(self.__state_lock):
                self._wait.on.wait()
            if self._wait.callback is None:
                self._next_transition = self._perform_finish
            else:
                self._next_transition = self._perform_resume
        except Interrupted:
            self._next_transition = self._perform_wait

    def _perform_resume(self):
        """
        Messages issued:
         - on_resume
         - on_run
        """
        assert self.state is ProcessState.WAITING

        self._call_with_super_check(self.on_resume)

        w = self._wait
        self._wait = None
        self._to_running()
        with _Unlock(self.__state_lock):
            self._return_value = w.callback(w.on)

        # Figure out what to do next
        if self._is_wait_retval(self._return_value):
            self._set_wait(self._return_value[0], self._return_value[1])
            self._next_transition = self._perform_wait
        else:
            self._next_transition = self._perform_finish

    def _perform_finish(self):
        """
        Messages issued:
         - on_finish
         - on_stop
        """
        assert self.state is ProcessState.RUNNING

        self._call_with_super_check(self.on_finish)
        self._to_stopped()

    def _perform_abort(self):
        """
        Messages issued:
         - on_abort
        """
        self._call_with_super_check(self.on_abort)

        self._to_stopped()
        self.__aborting_protect = False

    def _perform_fail(self, exception):
        """
        Messages issued:
         - on_fail
        :param exception: The exception that caused the failure
        :type exception: :class:`BaseException`
        """
        self._state = ProcessState.FAILED
        self._exception = exception
        self._next_transition = None
        try:
            self.__called = False
            self.on_fail()
        except BaseException as e:
            self._logger.warn(
                "There was an exception raised when calling on_fail "
                "to inform the process that an exception had been "
                "raised during execution.  Msg: {}".format(e.message))
        else:
            assert self.__called, \
                "on_fail was not called\n" \
                "Hint: Did you forget to call the superclass method?"

    def _to_running(self):
        self._state = ProcessState.RUNNING
        self._call_with_super_check(self.on_run)

    def _to_stopped(self):
        self._state = ProcessState.STOPPED
        self._call_with_super_check(self.on_stop)
        self._next_transition = None

    def _set_wait(self, wait_on, callback):
        self._wait = Wait(wait_on, callback)

    def _interrupt(self):
        try:
            self._wait.on.interrupt()
        except AttributeError:
            pass

    def _call_with_super_check(self, fn, *args, **kwargs):
        self.__called = False
        fn(*args, **kwargs)
        assert self.__called, \
            "{} was not called\n" \
            "Hint: Did you forget to call the superclass method?".format(fn.__name__)

    ###########################################################################

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

    @abstractmethod
    def _run(self, **kwargs):
        pass


class FunctionProcess(Process):
    # These will be replaced by build
    _output_name = None
    _func_args = None
    _func = None

    @classmethod
    def build(cls, func, output_name="value"):
        import inspect

        args, varargs, keywords, defaults = inspect.getargspec(func)

        def _define(cls, spec):
            for i in range(len(args)):
                default = None
                if defaults and len(defaults) - len(args) + i >= 0:
                    default = defaults[i]
                spec.input(args[i], default=default)

            spec.output(output_name)

        return type(func.__name__, (FunctionProcess,),
                    {Process.define.__name__: classmethod(_define),
                     '_func': func,
                     '_func_args': args,
                     '_output_name': output_name})

    def __init__(self):
        super(FunctionProcess, self).__init__()

    def _run(self, **kwargs):
        args = []
        for arg in self._func_args:
            args.append(kwargs.pop(arg))

        self.out(self._output_name, self._func(*args))
