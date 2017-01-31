# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
import time
from collections import Sequence
from plum.persistence.bundle import Bundle
from plum.wait import WaitOn
from plum.util import override
from plum.process_listener import ProcessListener
from plum.process import ProcessState
from plum.process_monitor import MONITOR


class Checkpoint(WaitOn):
    """
    This WaitOn doesn't actually wait, it's just a way to ask the engine to
    create a checkpoint at this point in the execution of a Process.
    """
    def init(self):
        super(Checkpoint, self).init()
        self.done(True)


class _CompoundWaitOn(WaitOn):
    __metaclass__ = ABCMeta

    WAIT_LIST = 'wait_list'

    def init(self, wait_list):
        super(_CompoundWaitOn, self).init()
        self._wait_list = wait_list

    @override
    def save_instance_state(self, out_state):
        super(self.__class__, self).save_instance_state(out_state)
        # Save all the waits lists
        waits = []
        for w in self._wait_list:
            b = Bundle()
            w.save_instance_state(b)
            waits.append(b)
        out_state[self.WAIT_LIST] = waits

    def load_instance_state(self, bundle):
        super(_CompoundWaitOn, self).load_instance_state(bundle)

        # Don't bother loading the children if we've finished
        if not self.is_done():
            self._wait_list = \
                [WaitOn.create_from(b) for b in bundle[self.WAIT_LIST]]
        else:
            self._wait_list = []


class WaitOnAll(_CompoundWaitOn):
    @override
    def init(self, wait_list):
        super(WaitOnAll, self).init(wait_list)

    @override
    def load_instance_state(self, bundle):
        super(WaitOnAll, self).load_instance_state(bundle)

    @override
    def wait(self, timeout=None):
        t0 = time.time()
        for w in self._wait_list:
            if not w.wait(timeout - (time.time() - t0)
                          if timeout is not None else None):
                # We timed out
                return False

        self.done(True)
        return True


class WaitOnAny(_CompoundWaitOn):
    @override
    def init(self, wait_list):
        super(WaitOnAny, self).init(wait_list)

    @override
    def load_instance_state(self, bundle):
        super(WaitOnAny, self).load_instance_state(bundle)

    @override
    def wait(self, timeout=None):
        t0 = time.time()
        while not self.is_done():
            if time.time() - t0 >= timeout:
                return False

            # Check if anyone is finished
            for w in self._wait_list:
                if w.wait(0):
                    self.done(True)
                    break

        return True


class WaitOnState(WaitOn, ProcessListener):
    WAIT_ON_PID = 'pid'
    WAIT_ON_STATE = 'state'

    @override
    def on_process_run(self, proc):
        if self._state is ProcessState.RUNNING:
            self._signal_done(proc)

    @override
    def on_process_wait(self, proc):
        if self._state is ProcessState.WAITING:
            self._signal_done(proc)

    @override
    def on_process_stop(self, proc):
        if self._state is ProcessState.STOPPED:
            self._signal_done(proc)

    @override
    def on_process_fail(self, proc):
        if self._state is ProcessState.FAILED:
            self._signal_done(proc)

    @override
    def save_instance_state(self, out_state):
        super(WaitOnState, self).save_instance_state(out_state)

        out_state[self.WAIT_ON_PID] = self._proc_pid
        out_state[self.WAIT_ON_STATE] = self._state

    @override
    def init(self, proc, state):
        """
        Create the WaitOnState.

        :param proc: The process to wait on
        :type proc: :class:`plum.process.Process`
        :param state: The state it needs to reach before being ready
        :type state: :class:`plum.process.ProcessState`
        """
        assert state in ProcessState
        super(WaitOnState, self).init()

        self._proc_pid = proc.pid
        self._state = state
        self._init_process(proc)

    @override
    def load_instance_state(self, bundle):
        super(WaitOnState, self).load_instance_state(bundle)

        self._proc_pid = bundle[self.WAIT_ON_PID]
        self._state = bundle[self.WAIT_ON_STATE]
        if not self.is_done():
            try:
                proc = MONITOR.get_process(self._proc_pid)
                self._init_process(proc)
            except ValueError:
                raise RuntimeError("The process that was being waited on is "
                                   "no longer running.")

    def _init_process(self, proc):
        proc.add_process_listener(self)
        if proc.state is self._state:
            self._signal_done(proc)

    def _signal_done(self, proc):
        try:
            self.done(True)
            proc.remove_process_listener(self)
        except RuntimeError:
            pass


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
        return WaitOnAll([WaitOnState(p, state) for p in proc]).wait(timeout)
    else:
        return WaitOnState(proc, state).wait(timeout)


class WaitOnProcess(WaitOnState):
    @override
    def init(self, proc):
        super(WaitOnProcess, self).init(proc, ProcessState.STOPPED)


class WaitOnProcessOutput(WaitOn, ProcessListener):
    WAIT_ON_PID = 'pid'
    OUTPUT_PORT = 'output_port'

    def init(self, proc, output_port):
        self._proc_pid = proc.pid
        self._output_port = output_port

        self._init_process(proc)

    @override
    def save_instance_state(self, out_state):
        super(WaitOnProcessOutput, self).save_instance_state(out_state)

        out_state[self.WAIT_ON_PID] = self._proc_pid
        out_state[self.OUTPUT_PORT] = self._output_port

    @override
    def load_instance_state(self, bundle):
        super(WaitOnProcessOutput, self).load_instance_state(bundle)

        self._proc_pid = bundle[self.WAIT_ON_PID]
        self._output_port = bundle[self.OUTPUT_PORT]
        if not self.is_done():
            try:
                proc = MONITOR.get_process(self._proc_pid)
                self._init_process(proc)
            except ValueError:
                raise RuntimeError("The process that was being waited on is "
                                   "no longer running.")

    def _init_process(self, proc):
        # If the process is in that state then we're done and don't need to
        # listen for anything else
        if self._output_port in proc.get_outputs():
            self.done(True)
        else:
            proc.add_process_listener(self)


def wait_until_stopped(proc, timeout=None):
    """
    Wait until a process or processes reach the STOPPED state.  `proc` can be
    a single process or a sequence of processes.

    :param proc: The process or sequence of processes to wait for
    :type proc: :class:`~plum.process.Process` or :class:`Sequence`
    :param state: The state to wait for
    :param timeout: The optional timeout
    """
    if isinstance(proc, Sequence):
        return WaitOnAll([WaitOnProcess(p) for p in proc]).wait(timeout)
    else:
        return WaitOnProcess(proc).wait(timeout)

