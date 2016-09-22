# -*- coding: utf-8 -*-

from abc import ABCMeta
import plum.knowledge_provider as process_database
from plum.persistence.bundle import Bundle
from plum.wait import WaitOn, WaitOnError, validate_callback_func
from plum.util import override


class Checkpoint(WaitOn):
    """
    This WaitOn doesn't actually wait, it's just a way to ask the engine to
    create a checkpoint at this point in the execution of a Process.
    """
    @classmethod
    def create_from(cls, saved_instance_state):
        return cls(saved_instance_state[WaitOn.BundleKeys.CALLBACK_NAME.value])

    @override
    def is_ready(self):
        return True


def checkpoint(callback):
    validate_callback_func(callback)
    return Checkpoint(callback.__name__)


class _CompoundWaitOn(WaitOn):
    __metaclass__ = ABCMeta

    WAIT_LIST = 'wait_list'

    @classmethod
    def create_from(cls, saved_instance_state):
        return cls(
            saved_instance_state[WaitOn.BundleKeys.CALLBACK_NAME.value],
            [WaitOn.create_from(b) for b in
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
    def is_ready(self):
        return all(w.is_ready() for w in self._wait_list)


def wait_on_all(callback, wait_list):
    validate_callback_func(callback)
    return WaitOnAll(callback.__name__, wait_list)


class WaitOnAny(_CompoundWaitOn):
    @override
    def is_ready(self):
        return any(w.is_ready() for w in self._wait_list)


def wait_on_any(callback, wait_list):
    validate_callback_func(callback)
    return WaitOnAny(callback.__name__, wait_list)


class WaitOnProcess(WaitOn):
    WAIT_ON_PID = 'pid'

    @classmethod
    def create_from(cls, bundle):
        return cls(bundle[WaitOn.BundleKeys.CALLBACK_NAME.value],
                   bundle[cls.WAIT_ON_PID])

    def __init__(self, callback_name, pid):
        super(WaitOnProcess, self).__init__(callback_name)
        self._pid = pid

    @override
    def is_ready(self):
        db = process_database.get_global_provider()
        if not db:
            raise WaitOnError(
                "Unable to check if process has finished because a global "
                "process database was not supplied.",
                WaitOnError.Nature.PERMANENT
            )
        return db.has_finished(self._pid)

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
    def create_from(cls, bundle):
        return cls(bundle[WaitOn.BundleKeys.CALLBACK_NAME.value],
                   bundle[cls.WAIT_ON_PID],
                   bundle[cls.OUTPUT_PORT])

    def __init__(self, callback_name, pid, output_port):
        super(WaitOnProcessOutput, self).__init__(callback_name)
        self._pid = pid
        self._output_port = output_port

    @override
    def is_ready(self):
        db = process_database.get_global_provider()
        if not db:
            raise WaitOnError(
                "Unable to check if process has finished because a global "
                "process database was not supplied.",
                WaitOnError.Nature.PERMANENT
            )

        try:
            db.get_output(self._pid, self._output_port)
            return True
        except KeyError:
            # If it can't find the output and the process has finished then we
            # can't continue
            if db.has_finished(self._pid):
                raise
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
