# -*- coding: utf-8 -*-

from abc import ABCMeta
import copy
import logging
import plum
import time
import uuid

from future.utils import with_metaclass

from plum.process_listener import ProcessListener
from plum.process_spec import ProcessSpec
from plum.utils import protected
from . import events
from . import futures
from . import base
from .base import Continue, Wait, Cancel, Stop, ProcessState, \
    TransitionFailed, Waiting
from . import process_comms
from . import stack
from . import utils

__all__ = ['Process', 'ProcessAction', 'ProcessMessage', 'ProcessState',
           'get_pid_from_bundle', 'Cancel', 'Wait', 'Stop', 'Continue',
           'TransitionFailed', 'Executor', 'Waiting']

_LOGGER = logging.getLogger(__name__)


class BundleKeys(object):
    """
    String keys used by the process to save its state in the state bundle.

    See :func:`save_instance_state` and :func:`load_instance_state`.
    """
    CREATION_TIME = 'CREATION_TIME'
    INPUTS = 'INPUTS'
    OUTPUTS = 'OUTPUTS'
    PID = 'PID'
    LOOP_CALLBACK = 'LOOP_CALLBACK'
    AWAITING = 'AWAITING'
    NEXT_STEP = 'NEXT_STEP'
    ABORT_MSG = 'ABORT_MSG'
    PROC_STATE = 'PROC_STATE'
    PAUSED = 'PAUSED'


class ProcessAction(object):
    """
    Actions that the process can be asked to perform
    These should be sent as the subject of a message to the process
    """
    PAUSE = 'pause'
    PLAY = 'play'
    ABORT = 'abort'
    REPORT_STATUS = 'report_status'


class ProcessMessage(object):
    """
    Messages that the process can emit, these will be the subject
    of the message
    """
    STATUS_REPORT = 'status_report'


class Running(base.Running):
    def enter(self):
        super(Running, self).enter()
        self.process.call_soon(self._run)

    def load_instance_state(self, process, saved_state):
        super(Running, self).load_instance_state(process, saved_state)
        if self.in_state:
            self.process.call_soon(self._run)

    def _run(self):
        # Guard, in case we left the state before the callback was actioned
        if self.in_state:
            with stack.in_stack(self.process):
                super(Running, self)._run()


class Executor(ProcessListener):
    _future = None
    _loop = None

    def __init__(self, interrupt_on_pause_or_wait=False):
        self._interrupt_on_pause_or_wait = interrupt_on_pause_or_wait

    def on_process_waiting(self, process, data):
        if self._interrupt_on_pause_or_wait:
            self._future.set_result('waiting')

    def on_process_paused(self, process):
        if self._interrupt_on_pause_or_wait:
            self._future.set_result('paused')

    def execute(self, process):
        process.add_process_listener(self)
        try:
            loop = process.loop()
            self._future = futures.Future()
            futures.chain(process.future(), self._future)

            def stop(x):
                loop.stop()

            self._future.add_done_callback(
                stop
                # lambda _: loop.stop()
            )

            if process.state in [ProcessState.CREATED, ProcessState.PAUSED]:
                process.play()

            loop.start()
            return self._future.result()
        finally:
            self._future = None
            self._loop = None
            process.remove_process_listener(self)


class Process(with_metaclass(ABCMeta, base.ProcessStateMachine)):
    """
    The Process class is the base for any unit of work in plum.

    A process can be in one of the following states:

    * CREATED
    * STARTED
    * RUNNING
    * WAITING
    * FINISHED
    * STOPPED
    * DESTROYED

    as defined in the :class:`ProcessState` enum.


    ::

    When a Process enters a state is always gets a corresponding message, e.g.
    on entering RUNNING it will receive the on_run message.  These are
    always called immediately after that state is entered but before being
    executed.
    """

    # Static class stuff ######################
    _spec_type = ProcessSpec

    @classmethod
    def get_state_classes(cls):
        states_map = super(Process, cls).get_state_classes()
        states_map[ProcessState.RUNNING] = Running
        return states_map

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

    @classmethod
    def recreate_from(cls, saved_state, *args, **kwargs):
        """"""
        """
        Recreate a process from a saved state, passing any positional and 
        keyword arguments on to load_instance_state

        :param args: The positional arguments for load_instance_state
        :param kwargs: The keyword arguments for load_instance_state
        :return: An instance of the object with its state loaded from the save state.
        """
        obj = cls.__new__(cls)
        obj.__init__(*args, **kwargs)
        base.call_with_super_check(obj.load_instance_state, saved_state)
        base.call_with_super_check(obj.init)
        return obj

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
        :type communicator: :class:`plum.Communicator`
        """
        # Don't allow the spec to be changed anymore
        self.spec().seal()

        # Input/output
        self._raw_inputs = None if inputs is None else utils.AttributesFrozendict(inputs)
        self._pid = pid
        self._logger = logger
        self._loop = loop if loop is not None else events.get_event_loop()
        self._communicator = communicator

        self._future = plum.Future()
        self._parsed_inputs = None
        self._outputs = {}
        self._uuid = None
        self.__event_helper = utils.EventHelper(ProcessListener)

        super(Process, self).__init__()

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
        if self.__logger is not None:
            return self.__logger
        else:
            return _LOGGER

    def loop(self):
        return self._loop

    def future(self):
        return self._future

    def has_finished(self):
        """
        Has the process finished i.e. completed running normally, without abort
        or an exception.

        :return: True if finished, False otherwise
        :rtype: bool
        """
        return self.done()

    def has_aborted(self):
        return self.cancelled()

    def save_instance_state(self, out_state):
        """
        Ask the process to save its current instance state.

        :param out_state: A bundle to save the state to
        :type out_state: :class:`plum.Bundle`
        """
        super(Process, self).save_instance_state(out_state)
        # Immutables first
        out_state[BundleKeys.CREATION_TIME] = self.creation_time
        out_state[BundleKeys.PID] = self.pid

        # Inputs/outputs
        if self.raw_inputs is not None:
            out_state[BundleKeys.INPUTS] = self.encode_input_args(self.raw_inputs)
        out_state[BundleKeys.OUTPUTS] = copy.deepcopy(self._outputs)

    @protected
    def load_instance_state(self, saved_state):
        # Set up runtime state
        super(Process, self).load_instance_state(saved_state)

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

        self._update_future()

    def add_process_listener(self, listener):
        assert (listener != self), "Cannot listen to yourself!"
        self.__event_helper.add_listener(listener)

    def remove_process_listener(self, listener):
        self.__event_helper.remove_listener(listener)

    @protected
    def set_logger(self, logger):
        self.__logger = logger

    @protected
    def log_with_pid(self, level, msg):
        self.logger.log(level, "{}: {}".format(self.pid, msg))

    # region Process messages

    def on_create(self):
        super(Process, self).on_create()

        # State stuff
        self.__CREATION_TIME = time.time()

        # Input/output
        self._check_inputs(self._raw_inputs)
        self._parsed_inputs = utils.AttributesFrozendict(self.create_input_args(self.raw_inputs))

        # Set up a process ID
        self._uuid = uuid.uuid4()
        if self._pid is None:
            self._pid = self._uuid

    def on_created(self):
        super(Process, self).on_created()
        base.call_with_super_check(self.init)

    @base.super_check
    def init(self):
        self.__logger = self._logger

        if self._communicator is not None:
            self._communicator.register_receiver(
                process_comms.ProcessReceiver(self), identifier=str(self.pid))

    def on_running(self):
        super(Process, self).on_running()
        self._fire_event(ProcessListener.on_process_running)

    def on_output_emitting(self, output_port, value):
        pass

    def on_output_emitted(self, output_port, value, dynamic):
        self.__event_helper.fire_event(ProcessListener.on_output_emitted,
                                       self, output_port, value, dynamic)

    def on_waiting(self, data):
        super(Process, self).on_waiting(data)
        self._fire_event(ProcessListener.on_process_waiting, data)

    def on_paused(self):
        super(Process, self).on_paused()
        self._fire_event(ProcessListener.on_process_paused)

    def on_finish(self, result):
        super(Process, self).on_finish(result)
        self._check_outputs()
        self.future().set_result(result)

    def on_finished(self):
        super(Process, self).on_finished()
        self._fire_event(ProcessListener.on_process_finished, self.result())

    def on_failed(self, exc_info):
        super(Process, self).on_failed(exc_info)
        self.future().set_exc_info(exc_info)
        self._fire_event(ProcessListener.on_process_failed, exc_info)

    def on_cancelled(self, msg):
        super(Process, self).on_cancelled(msg)
        self.future().cancel()
        self._fire_event(ProcessListener.on_process_cancelled, msg)

    # endregion

    def run(self):
        return self._run()

    def execute(self, return_on_idle=False):
        return Executor(return_on_idle).execute(self)

    @protected
    def out(self, output_port, value):
        self.on_output_emitting(output_port, value)
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
                if port.has_default():
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
        :class:`plum.Bundle`

        :param inputs: A mapping of the inputs as passed to the process
        :return: The encoded inputs
        """
        return inputs

    @protected
    def decode_input_args(self, encoded):
        """
        Decode saved input arguments as they came from the saved instance state
        :class:`plum.Bundle`

        :param encoded:
        :return: The decoded input args
        """
        return encoded

    def message_received(self, subject, body=None, sender_id=None):
        super(Process, self).message_received(subject, body, sender_id)
        if subject == ProcessAction.ABORT:
            self.abort()
        elif subject == ProcessAction.PAUSE:
            self.pause()
        elif subject == ProcessAction.PLAY:
            self.play()
        elif subject == ProcessAction.REPORT_STATUS:
            self._status_requested(
                self.loop(), subject, body, self.uuid, sender_id
            )

    def call_soon(self, callback, *args, **kwargs):
        """ Schedule a callback as soon as possible """
        self._loop.add_callback(callback, *args, **kwargs)

    def call_later(self, delay, callback, *args, **kwargs):
        """ Schedule a callback delayed by some time in seconds """
        self._loop.call_later(delay, callback, *args, **kwargs)

    def call_at(self, when, callback, *args, **kwargs):
        """ Schedule a callback at a particular time """
        self._loop.call_at(when, callback, *args, **kwargs)

    # region State entry/exit events

    def _fire_event(self, event, *args, **kwargs):
        self.__event_helper.fire_event(event, self, *args, **kwargs)

    # endregion

    def _send_message(self, subject, body=None, to=None):
        body_ = {'uuid': self.uuid}
        if body is not None:
            body_.update(body)
        self.send_message(subject, to=to, body=body_)

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

    def _update_future(self):
        if self.state == ProcessState.FINISHED:
            self._future.set_result(self.outputs)
        elif self.state == ProcessState.CANCELLED:
            self._future.cancel()
        elif self.state == ProcessState.FAILED:
            self._future.set_exception(self._state.exception)


def get_pid_from_bundle(process_bundle):
    return process_bundle[BundleKeys.PID]
