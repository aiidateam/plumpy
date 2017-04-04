# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
import time
from collections import Sequence
from plum.persistence.bundle import Bundle
from plum.wait import WaitOn, Unsavable
from plum.util import override
from plum.process_listener import ProcessListener
from plum.process import ProcessState
from plum.exceptions import TimeoutError, Unsupported


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
        for w in wait_list:
            if not isinstance(w, WaitOn):
                raise ValueError(
                    "Must provide objects of type WaitOn, got '{}'.".format(
                        w.__class__.__name__))
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

    @override
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


class WaitOnState(WaitOn, Unsavable, ProcessListener):
    WAIT_ON_PID = 'pid'
    WAIT_ON_STATE = 'state'

    @override
    def __init__(self, proc, state):
        """
        Create the WaitOnState.

        :param proc: The process to wait on
        :type proc: :class:`plum.process.Process`
        :param state: The state it needs to reach before being ready
        :type state: :class:`plum.process.ProcessState`
        """
        assert state in ProcessState
        super(WaitOnState, self).__init__()

        self._proc = proc
        self._state = state
        if self._proc.state is self._state:
            self.done()
        else:
            self._proc.add_process_listener(self)

    @override
    def interrupt(self):
        super(WaitOnState, self).interrupt()
        self._proc.remove_process_listener(self)

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

    def _signal_done(self, proc):
        try:
            self.done()
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


class WaitOnProcess(WaitOn, Unsavable, ProcessListener):
    def __init__(self, proc):
        """
        Wait for a process to terminate.

        :param proc: The process
        :type proc: :class:`plum.process.Process`
        """
        super(WaitOnProcess, self).__init__()
        if proc.has_terminated():
            self.done()
        else:
            proc.add_process_listener(self)

    @override
    def on_process_fail(self, process):
        self.done()

    @override
    def on_process_stop(self, process):
        self.done()


class WaitOnProcessOutput(WaitOn, Unsavable, ProcessListener):
    WAIT_ON_PID = 'pid'
    OUTPUT_PORT = 'output_port'

    def __init__(self, proc, output_port):
        super(WaitOnProcessOutput, self).__init__()
        self._output_port = output_port

        # Check if process has emitted, otherwise listen
        if self._output_port in proc.get_outputs():
            self.done()
        else:
            proc.add_process_listener(self)

    @override
    def on_output_emitted(self, process, output_port, value, dynamic):
        if output_port == self._output_port:
            self.done()
            process.remove_process_listener(self)


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


class WaitForSignal(WaitOn):
    def continue_(self):
        self.done(True)
