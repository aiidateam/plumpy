# -*- coding: utf-8 -*-

import uuid
import plum.util as util
from abc import ABCMeta, abstractmethod
from plum.persistence.bundle import Bundle
from plum.process_listener import ProcessListener
from plum.process_monitor import monitor
from plum.process_spec import ProcessSpec
from plum.util import protected


class Process(object):
    __metaclass__ = ABCMeta

    # Static class stuff ######################
    _spec_type = ProcessSpec
    _INPUTS = 'INPUTS'

    @classmethod
    def _define(cls, spec):
        pass

    @classmethod
    def spec(cls):
        try:
            return cls._spec
        except AttributeError:
            cls._spec = cls._spec_type()
            cls._define(cls._spec)
            return cls._spec

    @classmethod
    def get_name(cls):
        return cls.__name__

    @classmethod
    def _create_default_exec_engine(cls):
        """
        Crate the default execution engine.  Used if the run() method is
        called instead of asking an execution engine to run this process.

        :return: An instance of ExceutionEngine.
        """
        from plum.engine.serial import SerialEngine
        return SerialEngine()

    @classmethod
    def run(cls, inputs=None, exec_engine=None):
        if inputs is None:
            inputs = {}
        if not exec_engine:
            exec_engine = cls._create_default_exec_engine()
        return exec_engine.submit(cls, inputs).result()

    ############################################

    def __init__(self):
        # Don't allow the spec to be changed anymore
        self.spec().seal()

        self._pid = None
        self._inputs = None
        self._exec_engine = None
        self._process_registry = None
        self._output_values = {}
        self.__event_helper = util.EventHelper(ProcessListener)

        # Flags to make sure all the necessary event methods were called
        self._called = False

    @property
    def pid(self):
        return self._pid

    @property
    def inputs(self):
        return self._inputs

    def get_last_outputs(self):
        return self._output_values

    def save_instance_state(self, bundle):
        if self._inputs is not None:
            bundle[self._INPUTS] = Bundle(self._inputs)
        else:
            bundle[self._INPUTS] = None

    def add_process_listener(self, listener):
        assert (listener != self)
        self.__event_helper.add_listener(listener)

    def remove_process_listener(self, listener):
        self.__event_helper.remove_listener(listener)

    # Signalling messages ######################################################
    # Methods that signal events have happened, these should be called by the
    # external processes driving the Process (usually the engine)
    def perform_create(self, pid=None, inputs=None, saved_instance_state=None):
        if pid is None:
            pid = uuid.uuid1()

        self._called = False
        self.on_create(pid, inputs, saved_instance_state)
        assert self._called, \
            "on_create was not called\n" \
            "Hint: Did you forget to call the superclass method?"
        monitor.process_created(self)

    def perform_run(self, exec_engine, registry):
        self._exec_engine = exec_engine
        self._process_registry = registry

        self._called = False
        self.on_run()
        assert self._called, \
            "on_run was not called\n" \
            "Hint: Did you forget to call the superclass method?"

    def perform_wait(self, wait_on):
        self._called = False
        self.on_wait(wait_on)
        assert self._called, \
            "on_wait was not called\n" \
            "Hint: Did you forget to call the superclass method?"

    def perform_continue(self, wait_on):
        self._called = False
        self.on_continue(wait_on)
        assert self._called, \
            "on_continue was not called\n" \
            "Hint: Did you forget to call the superclass method?"

        self.perform_run(self.get_exec_engine(), self._process_registry)

    def perform_finish(self, retval):
        self._called = False
        self.on_finish(retval)
        assert self._called, \
            "on_finish was not called\n" \
            "Hint: Did you forget to call the superclass method?"

    def perform_stop(self):
        self._called = False
        self.on_stop()
        assert self._called, \
            "on_stop was not called\n" \
            "Hint: Did you forget to call the superclass method?"

    def perform_destroy(self):
        self._called = False
        self.on_destroy()
        assert self._called, \
            "on_destroy was not called\n" \
            "Hint: Did you forget to call the superclass method?"

    ############################################################################

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
        self._pid = pid
        if inputs is None:
            inputs = {}
        self._check_inputs(inputs)
        self._inputs = util.AttributesFrozendict(inputs)
        self._called = True

    @protected
    def on_run(self):
        """
        Called when the inputs of a process passed checks and the process
        is about to begin.

        Any class overriding this method should make sure to call the super
        method, usually at the end of the function.

        """
        self.__event_helper.fire_event(ProcessListener.on_process_run, self)
        self._called = True

    @protected
    def on_wait(self, wait_on):
        self.__event_helper.fire_event(
            ProcessListener.on_process_wait, self, wait_on)
        self._called = True

    @protected
    def on_continue(self, wait_on):
        self.__event_helper.fire_event(
            ProcessListener.on_process_continue, self, wait_on)
        self._called = True

    @protected
    def on_finish(self, retval):
        """
        Called when the process has finished and the outputs have passed
        checks
        :param retval: The return value from the process
        """
        self._check_outputs()
        self.__event_helper.fire_event(
            ProcessListener.on_process_finish, self, retval)
        self._called = True

    @protected
    def on_stop(self):
        self.__event_helper.fire_event(ProcessListener.on_process_stop, self)
        self._called = True

    @protected
    def on_destroy(self):
        """
        Called when the process has completed execution, and is being destroyed.
        """
        self.__event_helper.fire_event(ProcessListener.on_process_destroy, self)
        self._called = True

    def _on_output_emitted(self, output_port, value, dynamic):
        self.__event_helper.fire_event(ProcessListener.on_output_emitted,
                                       self, output_port, value, dynamic)

    #####################################################################

    @protected
    def get_exec_engine(self):
        return self._exec_engine

    @property
    @protected
    def process_registry(self):
        return self._process_registry

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

        self._output_values[output_port] = value
        self._on_output_emitted(output_port, value, dynamic)

    @protected
    def submit(self, process_class, inputs):
        return self.get_exec_engine().submit(process_class, inputs)

    @protected
    def run_from(self, checkpoint):
        return self.get_exec_engine().run_from(checkpoint)

    # Inputs ##################################################################
    def _create_input_args(self, inputs):
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
            ins = inputs.copy()
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

    def _check_inputs(self, inputs):
        # Check the inputs meet the requirements
        if not self.spec().has_dynamic_input():
            unexpected = set(inputs.iterkeys()) - set(
                self.spec().inputs.iterkeys())
            if unexpected:
                raise ValueError(
                    "Unexpected inputs found: {}.  If you want to allow dynamic"
                    " inputs add dynamic_input() to the spec definition.".
                    format(unexpected))

        for name, port in self.spec().inputs.iteritems():
            valid, msg = port.validate(inputs.get(name, None))
            if not valid:
                raise TypeError(
                    "Cannot run process '{}' because {}".
                    format(self.get_name(), msg))

    ###########################################################################

    # Outputs #################################################################
    def _check_outputs(self):
        # Check that the necessary outputs have been emitted
        for name, port in self.spec().outputs.iteritems():
            valid, msg = port.validate(self._output_values.get(name, None))
            if not valid:
                raise RuntimeError("Process {} failed because {}".
                                   format(self.get_name(), msg))

    ############################################################################

    def do_run(self):
        return self._run(**self._create_input_args(self.inputs))

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
                    {'_define': classmethod(_define),
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
