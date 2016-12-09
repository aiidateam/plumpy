# -*- coding: utf-8 -*-

import uuid
from enum import Enum
from abc import ABCMeta, abstractmethod
import threading

import plum.knowledge_provider as knowledge_provider
import plum.error as error
from plum.persistence.bundle import Bundle
from plum.process_listener import ProcessListener
from plum.process_monitor import MONITOR
from plum.process_spec import ProcessSpec
from plum.util import protected
import plum.util as util
from plum._base import LOGGER


class ProcessState(Enum):
    """
    The possible states that a :class:`Process` can be in.
    """
    CREATED = 0
    STARTED = 1
    RUNNING = 2
    FINISHED = 3
    STOPPED = 4
    DESTROYED = 5


class Process(object):
    """
    The Process class is the base for any unit of work in the plum workflow
    engine.
    A process can be in one of the following states:

    * CREATED
    * STARTED
    * RUNNING
    * FINISHED
    * STOPPED
    * DESTROYED

    as defined in the :class:`ProcessState` enum.

    The possible transitions between states are::

        CREATED -- STARTED -- RUNNING -- FINISHED -- STOPPED -- DESTROYED
           \           \_________|____________________/            /
            \_____________________________________________________/
    ::

    When a Process enters a state is always gets a corresponding message, e.g.
    on entering FINISHED it will receive the on_finish message.  These are
    always called immediately before that state is entered.

    """
    __metaclass__ = ABCMeta

    # Static class stuff ######################
    _spec_type = ProcessSpec

    class BundleKeys(Enum):
        """
        String keys used by the process to save its state in the state bundle.

        See create_from, on_save_instance_state and _load_instance_state.
        """
        CLASS_NAME = 'class_name'
        INPUTS = 'inputs'
        OUTPUTS = 'outputs'
        PID = 'pid'

    @staticmethod
    def instantiate(ProcClass, logger=None):
        p = ProcClass()
        if logger:
            p.set_logger(logger)
        else:
            p.set_logger(LOGGER)
        return p

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
        :return: An instance of this process with its state loaded from the save state.
        :rtype: :class:`Process`
        """
        # Get the class using the class loader and instantiate it
        class_name = saved_instance_state[cls.BundleKeys.CLASS_NAME.value]
        ProcClass =\
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
            cls._called = False
            cls.define(cls._spec)
            assert cls._called, \
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
        p.start()
        return p.outputs

    @classmethod
    def define(cls, spec):
        cls._called = True

    ############################################

    def __init__(self):
        # Don't allow the spec to be changed anymore
        self.spec().seal()

        self._logger = None

        # State stuff
        self._state = None
        self._pid = None
        self._finished = False
        self._state_transition_lock = threading.Lock()

        self._aborting = False
        self._aborted = False
        self._abort_lock = threading.Lock()

        # Input/output
        self._raw_inputs = None
        self._parsed_inputs = None
        self._outputs = {}

        # Events and running
        self.__event_helper = util.EventHelper(ProcessListener)

        # Flag to make sure all the necessary event methods were called
        self._called = False

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
    def aborted(self):
        return self._aborted

    @property
    def logger(self):
        """
        Get the logger for this class.  Can be None.

        :return: The logger.
        :rtype: :class:`logging.Logger`
        """
        return self._logger

    def has_finished(self):
        return self._finished

    def save_instance_state(self, bundle):
        bundle[self.BundleKeys.CLASS_NAME.value] = util.fullname(self)
        bundle[self.BundleKeys.PID.value] = self.pid

        # Save inputs
        inputs = None
        if self._raw_inputs is not None:
            inputs = Bundle(self._raw_inputs)
        bundle[self.BundleKeys.INPUTS.value] = inputs
        bundle[self.BundleKeys.OUTPUTS.value] = Bundle(self._outputs)

    def start(self):
        """
        Start running the process.  The process should be in the CREATED state.
        """
        assert self.state is ProcessState.CREATED

        # Keep going until we run out of tasks
        while True:
            # Acquite the lock, it's up to the function to release it
            self._state_transition_lock.acquire()
            fn = self._next()
            if fn is None:
                self._state_transition_lock.release()
                break
            else:
                try:
                    fn()
                except BaseException as e:
                    self._perform_fail(e)
                    MONITOR.process_failed(self.pid)
                    raise

    def abort(self):
        # By acquiring this lock once and then not releasing it we guarantee
        # that abort is only called once
        if self._abort_lock.acquire(False):
            with self._state_transition_lock:
                # Now we know that the state won't change
                if self.state in [ProcessState.CREATED, ProcessState.STARTED]:
                    self._aborting = True
                elif self.state is ProcessState.RUNNING:
                    try:
                        self.interrupt()
                        self._aborting = True
                    except NotImplementedError:
                        pass

        return self._aborting

    def add_process_listener(self, listener):
        assert (listener != self)
        self.__event_helper.add_listener(listener)

    def remove_process_listener(self, listener):
        self.__event_helper.remove_listener(listener)

    @protected
    def set_logger(self, logger):
        self._logger = logger

    # Process messages #####################################################
    # These should only be called by an execution engine (or tests)
    # Make sure to call the superclass method if your override any of these
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

        self._called = True

    @protected
    def on_start(self):
        """
        Called when the process is about to start for the first time.


        Any class overriding this method should make sure to call the super
        method, usually at the end of the function.

        """
        self.__event_helper.fire_event(ProcessListener.on_process_start, self)
        self._called = True

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
        self.__event_helper.fire_event(ProcessListener.on_process_run, self)
        self._called = True

    @protected
    def on_abort(self):
        """
        Called when the process has been asked to abort itself.
        """
        self._aborted = True
        self._called = True

    @protected
    def on_finish(self):
        """
        Called when the process has finished and the outputs have passed
        checks
        """
        self._check_outputs()
        self.__event_helper.fire_event(ProcessListener.on_process_finish, self)
        self._called = True

    @protected
    def on_stop(self):
        self.__event_helper.fire_event(ProcessListener.on_process_stop, self)
        self._called = True

    @protected
    def on_destroy(self):
        """
        Called after the process has stopped.  This event should be used to
        clean up any final resource, etc., still being held.
        """
        self.__event_helper.fire_event(ProcessListener.on_process_destroy, self)
        self._called = True

    @protected
    def on_fail(self, exception):
        """
        Called if the process raised an exception.
        :param exception: The exception that was raised
        :type exception: :class:`BaseException`
        """
        self._called = True

    def _on_output_emitted(self, output_port, value, dynamic):
        self.__event_helper.fire_event(ProcessListener.on_output_emitted,
                                       self, output_port, value, dynamic)

    #####################################################################

    @protected
    def do_run(self):
        try:
            return self.fast_forward()
        except error.FastForwardError:
            return self._run(**self._parsed_inputs)

    @protected
    def interrupt(self):
        """
        Called when the process has been asked to abort itself.  This could be
        by an external entity in which case the call will come from a different
        thread or it could be from the process itself, in which case it will
        be from the process thread.

        In either case the process has opportunity to stop what it's doing and
        return from the run method.  The next state change will be to STOPPED.
        """
        raise NotImplementedError("This process cannot be interrupted")

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

        inputs = bundle.get(self.BundleKeys.INPUTS.value, None)
        if inputs is not None:
            self._raw_inputs = util.AttributesFrozendict(inputs)

        self._outputs = bundle[self.BundleKeys.OUTPUTS.value].get_dict_deepcopy()

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
                        "Value not supplied for required inputs port {}".format(
                            name))

        return ins

    # State transition methods ################################################
    def _next(self):
        """
        Method to get the next method to run as part of the Process lifecycle.

        :return: A callable that only takes the self process as argument.
          May be None if the process has been destroyed.
        """
        # Use this while figuring out which state to go to next.  Methods
        # from other threads can also use this lock if they want to make sure
        # no state related changes will be made as they executed
        if self.state is ProcessState.CREATED:
            if self._aborting:
                return self._perform_destroy
            else:
                return self._perform_start
        elif self.state is ProcessState.STARTED:
            if self._aborting:
                return self._perform_stop
            else:
                return self._perform_run
        elif self.state is ProcessState.RUNNING:
            if self._aborting:
                return self._perform_stop
            else:
                return self._perform_finish
        elif self.state is ProcessState.FINISHED:
            return self._perform_stop
        elif self.state is ProcessState.STOPPED:
            return self._perform_destroy
        elif self.state is ProcessState.DESTROYED:
            return None
        else:
            raise RuntimeError(
                "Process is in unknown state '{}'.".format(self.state))

    def _perform_create(self, pid=None, inputs=None, saved_instance_state=None):
        if saved_instance_state is not None:
            self.load_instance_state(saved_instance_state)
        else:
            if pid is None:
                pid = uuid.uuid1()
            self._pid = pid
            self._check_inputs(inputs)
            if inputs is not None:
                self._raw_inputs = util.AttributesFrozendict(inputs)

        self._parsed_inputs =\
            util.AttributesFrozendict(self.create_input_args(self.raw_inputs))

        self._called = False
        self.on_create(pid, inputs, saved_instance_state)
        assert self._called, \
            "on_create was not called\n" \
            "Hint: Did you forget to call the superclass method?"

        self._state = ProcessState.CREATED

    def _perform_start(self):
        """
        Perform the state transition from CREATED -> STARTED.
        Messages issued:
         - on_start
        """
        assert self.state is ProcessState.CREATED

        MONITOR.process_starting(self)

        self._called = False
        self.on_start()
        assert self._called, \
            "on_run was not called\n" \
            "Hint: Did you forget to call the superclass method?"

        self._state = ProcessState.STARTED
        self._state_transition_lock.release()

    def _perform_run(self):
        """
        Messages issued:
         - on_run
        """
        assert self.state in [ProcessState.STARTED]

        self._called = False
        self.on_run()
        assert self._called, \
            "on_run was not called\n" \
            "Hint: Did you forget to call the superclass method?"

        self._state = ProcessState.RUNNING
        self._state_transition_lock.release()
        self._return_value = self.do_run()

    def _perform_finish(self):
        assert self.state is ProcessState.RUNNING

        self._called = False
        self.on_finish()
        assert self._called, \
            "on_finish was not called\n" \
            "Hint: Did you forget to call the superclass method?"

        self._finished = True
        self._state = ProcessState.FINISHED
        self._state_transition_lock.release()

    def _perform_stop(self):
        assert self.state in [ProcessState.STARTED, ProcessState.RUNNING,
                              ProcessState.FINISHED]

        if self._aborting and not self._aborted:
            self._called = False
            self.on_abort()
            assert self._called, \
                "on_abort was not called\n" \
                "Hint: Did you forget to call the superclass method?"

        self._called = False
        self.on_stop()
        assert self._called, \
            "on_stop was not called\n" \
            "Hint: Did you forget to call the superclass method?"

        self._state = ProcessState.STOPPED
        self._state_transition_lock.release()

        self.__event_helper.fire_event(
            ProcessListener.on_process_stopped, self)

    def _perform_destroy(self):
        assert self.state in [ProcessState.CREATED, ProcessState.STOPPED]

        if self._aborting and not self._aborted:
            self._called = False
            self.on_abort()
            assert self._called, \
                "on_abort was not called\n" \
                "Hint: Did you forget to call the superclass method?"

        self._called = False
        self.on_destroy()
        assert self._called, \
            "on_destroy was not called\n" \
            "Hint: Did you forget to call the superclass method?"

        self._state = ProcessState.DESTROYED
        self._state_transition_lock.release()

    def _perform_fail(self, exception):
        self._called = False
        try:
            self.on_fail(exception)
        except BaseException as e:
            # TODO: Log here that there was an exception raised while informing
            # the process that it had failed
            pass
        assert self._called, \
            "on_fail was not called\n" \
            "Hint: Did you forget to call the superclass method?"

    ###########################################################################

    def _check_inputs(self, inputs):
        # Check the inputs meet the requirements
        valid, msg = self.spec().validate(inputs)
        if not valid:
            raise ValueError(msg)

    ###########################################################################

    # Outputs #################################################################
    def _check_outputs(self):
        # Check that the necessary outputs have been emitted
        for name, port in self.spec().outputs.iteritems():
            valid, msg = port.validate(self._outputs.get(name, None))
            if not valid:
                raise RuntimeError("Process {} failed because {}".
                                   format(self.get_name(), msg))

    ############################################################################

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

