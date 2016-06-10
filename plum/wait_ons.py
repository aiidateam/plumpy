# -*- coding: utf-8 -*-

from abc import ABCMeta
from plum.process import ProcessListener
from plum.persistence.bundle import Bundle
from plum.wait import WaitOn
from plum.util import override


class Checkpoint(WaitOn):
    """
    This WaitOn doesn't actually wait, it's just a way to ask the engine to
    create a checkpoint at this point in the execution of a Process.
    """
    @classmethod
    def create_from(cls, saved_instance_state, process_manager):
        return cls(saved_instance_state[cls.CALLBACK_NAME])

    @override
    def is_ready(self, process_registry=None):
        return True


class _CompoundWaitOn(WaitOn):
    __metaclass__ = ABCMeta

    WAIT_LIST = 'wait_list'

    @classmethod
    def create_from(cls, saved_instance_state, process_manager):
        return cls(
            saved_instance_state[cls.CALLBACK_NAME],
            [WaitOn.create_from(b, process_manager) for b in
             saved_instance_state[cls.WAIT_LIST]])

    def __init__(self, callback_name, wait_list):
        super(self.__class__, self).__init__(callback_name)
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


class WaitOnAll(_CompoundWaitOn):
    @override
    def is_ready(self, process_registry=None):
        return all(w.is_ready() for w in self._wait_list)


class WaitOnAny(_CompoundWaitOn):
    @override
    def is_ready(self, process_registry=None):
        return any(w.is_ready() for w in self._wait_list)


class WaitOnProcess(WaitOn):
    WAIT_ON_PID = 'pid'

    @classmethod
    def create_from(cls, bundle, process_manager):
        return cls(bundle[cls.CALLBACK_NAME],
                   process_manager.get_process(bundle[cls.WAIT_ON_PID]))

    def __init__(self, callback_name, process):
        super(WaitOnProcess, self).__init__(callback_name)
        self._finished = False
        self._pid = process.pid

    @override
    def is_ready(self, process_registry=None):
        if process_registry:
            raise RuntimeError(
                "Unable to check if process has finished because a registry"
                "was not supplied.")
        return process_registry.is_finished(self._pid)

    @override
    def save_instance_state(self, out_state):
        super(WaitOnProcess, self).save_instance_state(out_state)
        out_state[self.WAIT_ON_PID] = self._pid


class WaitOnProcessOutput(WaitOn):
    WAIT_ON_PID = 'pid'
    OUTPUT_PORT = 'output_port'

    @classmethod
    def create_from(cls, bundle, process_manager):
        return cls(bundle[cls.CALLBACK_NAME],
                   bundle[cls.WAIT_ON_PID],
                   bundle[cls.OUTPUT_PORT])

    def __init__(self, callback_name, output_port, process, pid=None):
        super(WaitOnProcessOutput, self).__init__(callback_name)
        if process is not None:
            self._pid = process.pid
        else:
            assert pid is not None
            self._pid = pid
        self._output_port = output_port

    @override
    def is_ready(self, process_registry=None):
        if process_registry:
            raise RuntimeError(
                "Unable to check if process has finished because a registry"
                "was not supplied.")
        return self._output_port in process_registry.get_outputs(self._pid)

    @override
    def save_instance_state(self, out_state):
        super(WaitOnProcessOutput, self).save_instance_state(out_state)
        out_state[self.WAIT_ON_PID] = self._pid
        out_state[self.OUTPUT_PORT] = self._output_port
