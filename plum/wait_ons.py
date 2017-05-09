# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
import time
import threading
from collections import Sequence
from plum.persistence.bundle import Bundle
from plum.wait import WaitOn, Unsavable, Interrupted, WaitException, create_from, WaitEvent, WaitOnEvent
from plum.util import override
from plum.process_listener import ProcessListener
from plum.process import ProcessState
from plum.exceptions import TimeoutError


class Checkpoint(WaitOn):
    """
    This WaitOn doesn't actually wait, it's just a way to ask the engine to
    create a checkpoint at this point in the execution of a Process.
    """

    @override
    def wait(self, timeout=None):
        return True

    @override
    def interrupt(self):
        pass


class _CompoundWaitOn(WaitOn):
    __metaclass__ = ABCMeta

    WAIT_LIST = 'wait_list'

    def __init__(self, wait_list):
        super(_CompoundWaitOn, self).__init__()
        for w in wait_list:
            if not isinstance(w, WaitOn):
                raise ValueError(
                    "Must provide objects of type WaitOn, got '{}'.".format(
                        w.__class__.__name__))

        self._wait_list = wait_list
        self._interrupted = False

    @override
    def save_instance_state(self, out_state):
        super(_CompoundWaitOn, self).save_instance_state(out_state)
        # Save all the waits lists
        waits = []
        for w in self._wait_list:
            b = Bundle()
            w.save_instance_state(b)
            waits.append(b)
        out_state[self.WAIT_LIST] = waits

    @override
    def load_instance_state(self, saved_state):
        super(_CompoundWaitOn, self).load_instance_state(saved_state)
        self._wait_list = [create_from(b) for b in saved_state[self.WAIT_LIST]]
        self._interrupted = False


class WaitOnAll(_CompoundWaitOn):
    @override
    def wait(self, timeout=None):
        self._interrupted = False
        t0 = time.time()
        remaining_timeout = timeout
        for w in self._wait_list:
            if self._interrupted:
                raise Interrupted()

            if not w.wait(remaining_timeout):
                # We timed out
                return False

            if timeout is not None:
                elapsed_time = time.time() - t0
                remaining_timeout = timeout - elapsed_time

        return True

    @override
    def interrupt(self):
        self._interrupted = True
        for w in self._wait_list:
            w.interrupt()


class WaitOnAny(_CompoundWaitOn):
    def __init__(self, wait_list):
        super(WaitOnAny, self).__init__(wait_list)
        self._interrupted = False

    @override
    def wait(self, timeout=None):
        self._interrupted = False
        t0 = time.time()
        while True:
            for w in self._wait_list:
                if self._interrupted:
                    raise Interrupted()

                if timeout is not None and time.time() - t0 >= timeout:
                    return False

                # Check if anyone is finished
                if w.wait(0):
                    return True

    @override
    def interrupt(self):
        self._interrupted = True


class WaitOnProcessState(WaitOn, Unsavable, ProcessListener):
    STATE_REACHED = 0
    STATE_UNREACHABLE = 1

    @override
    def __init__(self, proc, target_state):
        """
        Create the WaitOnState.

        :param proc: The process to wait on
        :type proc: :class:`plum.process.Process`
        :param target_state: The state it needs to reach before being ready
        :type target_state: :class:`plum.process.ProcessState`
        """
        assert target_state in ProcessState
        super(WaitOnProcessState, self).__init__()

        self._proc = proc
        self._target_state = target_state
        self._wait_outcome = None
        self._state_event = WaitOnEvent()

    @override
    def wait(self, timeout=None):
        self._wait_outcome = None
        with self._proc.listen_scope(self):
            state = self._proc.state
            # Are we currently in the state?
            if state == self._target_state:
                return True

            if self._is_target_state_unreachable(state):
                raise WaitException(
                    "Target state '{}' is unreachable from current state "
                    "'{}'".format(self._target_state, state)
                )

            if not self._state_event.wait(timeout):
                # Timed-out
                return False
            elif self._wait_outcome is self.STATE_REACHED:
                return True
            elif self._wait_outcome is self.STATE_UNREACHABLE:
                raise WaitException(
                    "Target state '{}' is unreachable".format(self._target_state)
                )

    @override
    def interrupt(self):
        self._state_event.interrupt()

    @override
    def on_process_run(self, proc):
        self._state_changed(ProcessState.RUNNING)

    @override
    def on_process_wait(self, proc):
        self._state_changed(ProcessState.WAITING)

    @override
    def on_process_stop(self, proc):
        self._state_changed(ProcessState.STOPPED)

    @override
    def on_process_fail(self, proc):
        self._state_changed(ProcessState.FAILED)

    def _state_changed(self, new_state):
        if new_state is self._target_state:
            self._set_outcome(self.STATE_REACHED)
        elif self._is_target_state_unreachable(new_state):
            self._set_outcome(self.STATE_UNREACHABLE)

    def _is_target_state_unreachable(self, current_state):
        # Check start state
        if self._target_state is ProcessState.CREATED and \
                        current_state != ProcessState.CREATED:
            return True

        # Check terminal states
        if current_state is ProcessState.STOPPED or \
                        current_state is ProcessState.FAILED:
            if current_state != self._target_state:
                return True

        return False

    def _set_outcome(self, outcome):
        if self._wait_outcome is None:
            self._wait_outcome = outcome
            self._state_event.set()


def wait_until(proc, state, timeout=None):
    """
    Wait until a process or processes reaches a certain state.  `proc` can be
    a single process or a sequence of processes.

    :param proc: The process or sequence of processes to wait for
    :type proc: :class:`plum.process.Process` or :class:`Sequence`
    :param state: The state to wait for
    :param timeout: The optional timeout
    """
    if isinstance(proc, Sequence):
        return WaitOnAll([WaitOnProcessState(p, state) for p in proc]).wait(timeout)
    else:
        return WaitOnProcessState(proc, state).wait(timeout)


class WaitOnProcess(WaitOnEvent, Unsavable, ProcessListener):
    def __init__(self, process):
        """
        Wait for a process to terminate.

        :param process: The process
        :type process: :class:`plum.process.Process`
        """
        super(WaitOnProcess, self).__init__()
        self._proc = process

    @override
    def wait(self, timeout=None):
        with self._proc.listen_scope(self):
            if self._proc.has_terminated():
                return True

            return super(WaitOnProcess, self).wait(timeout)

    @override
    def on_process_done_playing(self, process):
        if process.has_terminated():
            self.set()


class WaitOnProcessOutput(WaitOnEvent, Unsavable, ProcessListener):
    def __init__(self, process, output_port):
        """
        :param process: The process whose output is being waited for
        :type process: :class:`plum.process.Process`
        :param output_port: The output port being waited on
        :type output_port: str or unicode
        """
        super(WaitOnProcessOutput, self).__init__()
        self._proc = process
        self._output_port = output_port

    @override
    def wait(self, timeout=None):
        with self._proc.listen_scope(self):
            if self._output_port in self._proc.outputs:
                return True

            return super(WaitOnProcessOutput, self).wait(timeout)

    @override
    def on_output_emitted(self, process, output_port, value, dynamic):
        if output_port == self._output_port:
            self.set()


def wait_until_stopped(proc, timeout=None):
    """
    Wait until a process or processes reach the STOPPED state.  `proc` can be
    a single process or a sequence of processes.

    :param proc: The process or sequence of processes to wait for
    :type proc: :class:`~plum.process.Process` or :class:`Sequence`
    :param timeout: The optional timeout
    """
    return wait_until(proc, ProcessState.STOPPED, timeout)


class WaitRegion(object):
    """
    A WaitRegion is a context that will not exit until the wait on has finished
    or the (optional) timeout has been reached in which case an TimeoutError is
    raised.
    """

    def __init__(self, wait_on, timeout=None):
        self._wait_on = wait_on
        self._timeout = timeout

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._wait_on.wait(self._timeout):
            raise TimeoutError()


class Barrier(WaitOn):
    def __init__(self):
        super(Barrier, self).__init__()
        self._barrier = threading.Event()
        self._blocking = True

    @override
    def wait(self, timeout=None):
        if not self._barrier.wait(timeout):
            return False
        else:
            if self._blocking:
                # Must have been interrupted
                raise Interrupted()
            else:
                return True

    @override
    def interrupt(self):
        self._barrier.set()

    @override
    def save_instance_state(self, out_state):
        super(Barrier, self).save_instance_state(out_state)
        out_state['blocking'] = self._blocking

    @override
    def load_instance_state(self, saved_state):
        super(Barrier, self).load_instance_state(saved_state)
        self._barrier = threading.Event()
        self._blocking = saved_state['blocking']

    def continue_(self):
        self._blocking = False
        self._barrier.set()
