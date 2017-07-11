# -*- coding: utf-8 -*-

from abc import ABCMeta

from collections import Sequence
from plum.persistence.bundle import Bundle
from plum.event import WaitOnEvent, wait_on_process_event
from plum.wait import WaitOn, create_from
from plum.util import override
from plum.process_listener import ProcessListener
from plum.process import ProcessState


class Checkpoint(WaitOn):
    """
    This WaitOn doesn't actually wait, it's just a way to ask the engine to
    create a checkpoint at this point in the execution of a Process.
    """

    def __init__(self, loop):
        super(Checkpoint, self).__init__(loop)
        self.future().set_result(None)


class _CompoundWaitOn(WaitOn):
    __metaclass__ = ABCMeta

    WAIT_LIST = 'wait_list'

    def __init__(self, wait_list, loop):
        super(_CompoundWaitOn, self).__init__(loop)
        for w in wait_list:
            if not isinstance(w, WaitOn):
                raise ValueError(
                    "Must provide objects of type WaitOn, got '{}'.".format(w.__class__.__name__))

        self._wait_list = wait_list

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
    def load_instance_state(self, saved_state, loop):
        super(_CompoundWaitOn, self).load_instance_state(saved_state, loop)
        self._wait_list = [create_from(b, loop) for b in saved_state[self.WAIT_LIST]]


class WaitOnAll(_CompoundWaitOn):
    def __init__(self, wait_list, loop):
        super(WaitOnAll, self).__init__(wait_list, loop)
        self._num_finished = 0

        for wait_on in self._wait_list:
            wait_on.future().add_done_callback(self._wait_done)

    def load_instance_state(self, saved_state, loop):
        super(WaitOnAll, self).load_instance_state(saved_state, loop)
        self._num_finished = 0

    def _wait_done(self, future):
        self._num_finished += 1
        if self._num_finished == len(self._wait_list):
            self._future.set_result([w.future().result() for w in self._wait_list])


class WaitOnAny(_CompoundWaitOn):
    def __init__(self, wait_list, loop):
        super(WaitOnAny, self).__init__(wait_list, loop)

        for wait_on in self._wait_list:
            wait_on.future(loop).add_done_callback(self._wait_done)

    def _wait_done(self, future):
        if not self._future.done():
            self._future.set_result(None)


class WaitOnProcessState(WaitOn, ProcessListener):
    STATE_REACHED = 'state_reached'
    STATE_UNREACHABLE = 'state_unreachable'

    @override
    def __init__(self, proc, target_state, loop):
        """
        Create the WaitOnState.

        :param proc: The process to wait on
        :type proc: :class:`plum.process.Process`
        :param target_state: The state it needs to reach before being ready
        :type target_state: :class:`plum.process.ProcessState`
        """
        assert target_state in ProcessState, "Must supply a valid process state"
        super(WaitOnProcessState, self).__init__(loop)

        self._pid = proc.pid
        self._target_state = target_state

        proc = loop.get_object(self._pid)

        # Are we currently in the state?
        if proc.state == self._target_state:
            self.future().set_result(self.STATE_REACHED)
        elif self._is_target_state_unreachable(proc.state):
            self.future().set_result(self.STATE_UNREACHABLE)
        else:
            proc.add_process_listener(self)

    @override
    def on_process_run(self, proc):
        self._state_changed(proc)

    @override
    def on_process_wait(self, proc):
        self._state_changed(proc)

    @override
    def on_process_stop(self, proc):
        self._state_changed(proc)

    @override
    def on_process_fail(self, proc):
        self._state_changed(proc)

    def _state_changed(self, proc):
        if proc.state is self._target_state:
            self._future.set_result(self.STATE_REACHED)
        elif self._is_target_state_unreachable(proc.state):
            self._future.set_result(self.STATE_UNREACHABLE)

        if self._future.done():
            proc.remove_process_listener(self)

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
        wait_for = WaitOnAll([WaitOnProcessState(p, state, loop) for p in proc], loop)
    else:
        wait_for = WaitOnProcessState(proc, state, loop)

    results = loop.run_until_complete(wait_for.future())

    if any(result is WaitOnProcessState.STATE_UNREACHABLE for result in results):
        raise RuntimeError("State '{}' could not be reached for at least one process".format(state))


def wait_on_process(pid, loop):
    return wait_on_process_event(loop, pid, 'terminated')


class WaitOnProcessOutput(WaitOn):
    def __init__(self, pid, port, loop):
        """
        :param process: The process whose output is being waited for
        :type process: :class:`plum.process.Process`
        :param port: The output port being waited on
        :type port: str or unicode
        """
        super(WaitOnProcessOutput, self).__init__(loop)
        self._wait_on_event = wait_on_process_event(loop, pid, 'output_emitted.{}'.format(port))
        self._wait_on_event.future().add_done_callbacK(self._output_emitted)

    def load_instance_state(self, saved_state, loop):
        super(WaitOnProcessOutput, self).load_instance_state(saved_state, loop)
        self._wait_on_event = WaitOnEvent.create_from(loop, saved_state['output_event'])
        self._wait_on_event.future().add_done_callbacK(self._output_emitted)

    def save_instance_state(self, out_state):
        event_state = Bundle()
        self._wait_on_event.save_instance_state(event_state)
        out_state['output_event'] = event_state

    def _output_emitted(self, future):
        self.future().set_result(future.get_result()[1])


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
    @override
    def load_instance_state(self, saved_state, loop):
        super(Barrier, self).load_instance_state(saved_state, loop)
        if saved_state['is_open']:
            self.open()

    @override
    def save_instance_state(self, out_state):
        super(Barrier, self).save_instance_state(out_state)
        out_state['is_open'] = self.future().done()

    def open(self):
        if self._future.done():
            raise RuntimeError('Barrier already open')

        self._future.set_result('open')
