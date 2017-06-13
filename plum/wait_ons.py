# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
import time
import threading
from collections import Sequence
from plum.persistence.bundle import Bundle
from plum.wait import WaitOn, Unsavable, WaitException, create_from, WaitEvent, WaitOnEvent
from plum.util import override
from plum.process_listener import ProcessListener
from plum.process import ProcessState
from plum.exceptions import TimeoutError, Interrupted


class Checkpoint(WaitOn):
    """
    This WaitOn doesn't actually wait, it's just a way to ask the engine to
    create a checkpoint at this point in the execution of a Process.
    """

    def __init__(self):
        self._future = None

    @override
    def load_instance_state(self, saved_state):
        self._future = None

    @override
    def get_future(self, loop):
        if self._future is None:
            self._future = loop.create_future()
            self._future.set_result(None)

        return self._future


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
        self._future = None

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
        self._future = None


class WaitOnAll(_CompoundWaitOn):
    def __init__(self, wait_list):
        super(WaitOnAll, self).__init__(wait_list)
        self._num_finished = 0

    def load_instance_state(self, saved_state):
        super(WaitOnAll, self).load_instance_state(saved_state)
        self._num_finished = 0

    def get_future(self, loop):
        if not self._future:
            self._future = loop.create_future()
            for wait_on in self._wait_list:
                wait_on.get_future(loop).add_done_callback(self._wait_done)

        return self._future

    def _wait_done(self, future):
        self._num_finished += 1
        if self._num_finished == len(self._wait_list):
            self._future.set_result(None)


class WaitOnAny(_CompoundWaitOn):
    def get_future(self, loop):
        if not self._future:
            self._future = loop.create_future()
            for wait_on in self._wait_list:
                wait_on.get_future(loop).add_done_callback(self._wait_done)

        return self._future

    def _wait_done(self, future):
        if not self._future.done():
            self._future.set_result(None)


class WaitOnProcessState(WaitOn, ProcessListener):
    STATE_REACHED = 'state_reached'
    STATE_UNREACHABLE = 'state_unreachable'

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

        self._pid = proc.pid
        self._target_state = target_state
        self._future = None

    def get_future(self, loop):
        if self._future is None:
            future = loop.create_future()
            proc = loop.get_process(self._pid)

            # Are we currently in the state?
            if proc.state == self._target_state:
                future.set_result(self.STATE_REACHED)
            elif self._is_target_state_unreachable(proc.state):
                future.set_result(self.STATE_UNREACHABLE)
            else:
                proc.add_process_listener(self)

            self._future = future

        return self._future

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
            self._future.set_result(self.STATE_REACHED)
        elif self._is_target_state_unreachable(new_state):
            self._future.set_result(self.STATE_UNREACHABLE)

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


def run_until(proc, state, loop):
    """
    Run the event loop until a process or processes reaches a certain state.
    `proc` can be a single process or a sequence of processes.

    :param proc: The process or sequence of processes to wait for
    :type proc: :class:`plum.process.Process` or :class:`Sequence`
    :param state: The state to run until
    :param loop: The event loop
    """
    if isinstance(proc, Sequence):
        wait_for = WaitOnAll([WaitOnProcessState(p, state) for p in proc])
    else:
        wait_for = WaitOnProcessState(proc, state)

    return loop.run_until_complete(wait_for.get_future(loop))


class WaitOnProcess(WaitOnEvent):
    def __init__(self, process):
        """
        Wait for a process to terminate.

        :param process: The process
        :type process: :class:`plum.process.Process`
        """
        super(WaitOnProcess, self).__init__()
        self._pid = process.pid

    def get_future(self, loop):
        return loop.get_process_future(self._pid)

    def save_instance_state(self, out_state):
        super(WaitOnProcess, self).save_instance_state(out_state)
        out_state['pid'] = self._pid

    def load_instance_state(self, saved_state):
        super(WaitOnProcess, self).load_instance_state(saved_state)
        self._pid = saved_state['pid']


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
    return run_until(proc, ProcessState.STOPPED, timeout)


class Barrier(WaitOn):
    def __init__(self):
        super(Barrier, self).__init__()
        self._is_open = False
        self._future = None

    @override
    def save_instance_state(self, out_state):
        super(Barrier, self).save_instance_state(out_state)
        out_state['is_open'] = self._is_open

    @override
    def load_instance_state(self, saved_state):
        super(Barrier, self).load_instance_state(saved_state)
        self._is_open = saved_state['is_open']
        self._future = None

    def get_future(self, loop):
        if self._future is None:
            self._future = loop.create_future()
            if self._is_open:
                self._future.set_result('open')

        return self._future

    def open(self):
        self._is_open = True
        if self._future is not None:
            self._future.set_result('open')
