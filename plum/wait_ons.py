# -*- coding: utf-8 -*-

from abc import ABCMeta
from plum.process import Process
from plum.persistence.bundle import Bundle
from plum.wait import WaitOn, validate_callback_func
from plum.util import override


class Checkpoint(WaitOn):
    """
    This WaitOn doesn't actually wait, it's just a way to ask the engine to
    create a checkpoint at this point in the execution of a Process.
    """
    @classmethod
    def create_from(cls, saved_instance_state, process_factory):
        return cls(saved_instance_state[cls.CALLBACK_NAME])

    @override
    def is_ready(self, registry=None):
        return True


def checkpoint(callback):
    validate_callback_func(callback)
    return Checkpoint(callback.__name__)


class _CompoundWaitOn(WaitOn):
    __metaclass__ = ABCMeta

    WAIT_LIST = 'wait_list'

    @classmethod
    def create_from(cls, saved_instance_state, process_factory):
        return cls(
            saved_instance_state[cls.CALLBACK_NAME],
            [WaitOn.create_from(b, process_factory) for b in
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
    def is_ready(self, registry):
        return all(w.is_ready(registry) for w in self._wait_list)


def wait_on_all(callback, wait_list):
    validate_callback_func(callback)
    return WaitOnAll(callback.__name__, wait_list)


class WaitOnAny(_CompoundWaitOn):
    @override
    def is_ready(self, registry):
        return any(w.is_ready(registry) for w in self._wait_list)


def wait_on_any(callback, wait_list):
    validate_callback_func(callback)
    return WaitOnAny(callback.__name__, wait_list)


class WaitOnProcess(WaitOn):
    WAIT_ON_PID = 'pid'

    @classmethod
    def create_from(cls, bundle, process_factory):
        return cls(bundle[cls.CALLBACK_NAME],
                   bundle[cls.WAIT_ON_PID])

    def __init__(self, callback_name, pid):
        super(WaitOnProcess, self).__init__(callback_name)
        self._pid = pid

    @override
    def is_ready(self, registry):
        if not registry:
            raise RuntimeError(
                "Unable to check if process has finished because a registry "
                "was not supplied.")
        return registry.is_finished(self._pid)

    @override
    def save_instance_state(self, out_state):
        super(WaitOnProcess, self).save_instance_state(out_state)
        out_state[self.WAIT_ON_PID] = self._pid


def wait_on_process(callback, pid=None, future=None):
    assert pid is not None or future is not None, \
        "Must supply a pid or a future"
    validate_callback_func(callback)
    if pid is None:
        pid = future.pid
    return WaitOnProcess(callback.__name__, pid)


class WaitOnProcessOutput(WaitOn):
    WAIT_ON_PID = 'pid'
    OUTPUT_PORT = 'output_port'

    @classmethod
    def create_from(cls, bundle, process_factory):
        return cls(bundle[cls.CALLBACK_NAME],
                   bundle[cls.WAIT_ON_PID],
                   bundle[cls.OUTPUT_PORT])

    def __init__(self, callback_name, pid, output_port):
        super(WaitOnProcessOutput, self).__init__(callback_name)
        self._pid = pid
        self._output_port = output_port

    @override
    def is_ready(self, registry):
        if not registry:
            raise RuntimeError(
                "Unable to check if process has finished because a registry "
                "was not supplied.")
        try:
            registry.get_output(self._pid, self._output_port)
            return True
        except ValueError:
            return False

    @override
    def save_instance_state(self, out_state):
        super(WaitOnProcessOutput, self).save_instance_state(out_state)
        out_state[self.WAIT_ON_PID] = self._pid
        out_state[self.OUTPUT_PORT] = self._output_port


def wait_on_process_output(callback, output_port, pid=None, future=None):
    assert pid is not None or future is not None, \
        "Must supply a pid or a future"
    validate_callback_func(callback)
    if pid is None:
        pid = future.pid
    return WaitOnProcessOutput(callback.__name__, pid, output_port)
