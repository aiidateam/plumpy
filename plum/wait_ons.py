# -*- coding: utf-8 -*-

from abc import ABCMeta

import apricotpy
from apricotpy import persistable
from collections import Sequence
from plum.wait import WaitOn
from plum.utils import override
from plum.process import ProcessState
from . import process


class WaitOnProcessState(persistable.AwaitableLoopObject):
    STATE_REACHED = 'state_reached'
    STATE_UNREACHABLE = 'state_unreachable'

    @override
    def __init__(self, proc_uuid, target_state, loop=None):
        """
        Create the WaitOnState.

        :param proc_uuid: The process to PID wait on
        :param target_state: The state it needs to reach before being ready
        :type target_state: :class:`plum.process.ProcessState`
        """
        assert target_state in ProcessState, "Must supply a valid process state"
        super(WaitOnProcessState, self).__init__(loop)

        if isinstance(proc_uuid, process.Process):
            self.store._uuid = proc_uuid.uuid
        else:
            self.store._uuid = proc_uuid
        self._target_state = target_state

        self.execute()

    def save_instance_state(self, out_state):
        super(WaitOnProcessState, self).save_instance_state(out_state)
        out_state['target_state'] = self._target_state

    def load_instance_state(self, saved_state):
        super(WaitOnProcessState, self).load_instance_state(saved_state)
        self._target_state = saved_state['target_state']
        if not self.done():
            self.execute()

    def execute(self):
        # Listen to event messages from our target process
        self.loop().messages().add_listener(
            self._on_event_message,
            subject_filter="on_*"  # All event messages
        )
        # Ask the process to respond with state information
        self.send_message(to=self.store._uuid, subject=process.ProcessAction.REPORT_STATUS)

    def message_received(self, subject, body, sender_id):
        if self.done():
            return
        if sender_id == self.store._uuid and subject == process.ProcessMessage.STATUS_REPORT:
            self._state_changed(body['state'])

    def _on_event_message(self, loop, subject, to, body, sender_id):
        if self.done():
            return
        if sender_id == self.store._uuid:
            self._state_changed(body['state'])

    def _state_changed(self, state):
        if state is self._target_state:
            self.set_result(self.STATE_REACHED)
        elif not process.ProcessStateTransitions.is_reachable(state, self._target_state):
            self.set_result(self.STATE_UNREACHABLE)

        if self.done():
            self.disable_message_listening()
            self.loop().messages().remove_listener(self._on_event_message)


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
        wait_for = persistable.gather(
            (loop.create(WaitOnProcessState, p, state) for p in proc), loop
        )
    else:
        wait_for = loop.create(WaitOnProcessState, proc, state)

    results = loop.run_until_complete(wait_for)

    if any(result is WaitOnProcessState.STATE_UNREACHABLE for result in results):
        raise RuntimeError("State '{}' could not be reached for at least one process".format(state))


class Barrier(WaitOn):
    @override
    def load_instance_state(self, saved_state):
        super(Barrier, self).load_instance_state(saved_state)

        if saved_state['is_open']:
            self.open()

    @override
    def save_instance_state(self, out_state):
        super(Barrier, self).save_instance_state(out_state)
        out_state['is_open'] = self.done()

    def open(self):
        if self.done():
            raise RuntimeError('Barrier already open')

        self.set_result('open')
