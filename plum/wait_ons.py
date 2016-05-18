# -*- coding: utf-8 -*-

from abc import ABCMeta
from plum.process import ProcessListener
from plum.persistence.bundle import Bundle
from plum.wait import WaitOn


class _CompoundWaitOn(WaitOn):
    __metaclass__ = ABCMeta

    WAIT_LIST = 'wait_list'

    @classmethod
    def create_from(cls, bundle, exec_engine):
        return cls(
            bundle[cls.CALLBACK_NAME],
            [WaitOn.create_from(b, exec_engine) for b in bundle[cls.WAIT_LIST]])

    def __init__(self, callback_name, wait_list):
        super(self.__class__, self).__init__(callback_name)
        self._wait_list = wait_list

    def save_instance_state(self, bundle, exec_engine):
        super(self.__class__, self).save_instance_state(bundle, exec_engine)
        # Save all the waits lists
        waits = []
        for w in self._wait_list:
            b = Bundle()
            w.save_instance_state(b)
            waits.append(b)
        bundle[self.WAIT_LIST] = waits


class WaitOnAll(_CompoundWaitOn):
    def is_ready(self):
        return all(w.is_ready() for w in self._wait_list)


class WaitOnAny(_CompoundWaitOn):
    def is_ready(self):
        return any(w.is_ready() for w in self._wait_list)


class WaitOnProcess(WaitOn, ProcessListener):
    WAIT_ON_PID = 'pid'

    @classmethod
    def create_from(cls, bundle, exec_engine):
        return cls(bundle[cls.CALLBACK_NAME],
                   exec_engine.get_process(bundle[cls.WAIT_ON_PID]))

    def __init__(self, callback_name, process):
        super(WaitOnProcess, self).__init__(callback_name)
        self._finished = False
        self._process = process
        process.add_process_listener(self)

    def on_process_finished(self, process, retval):
        assert self._process is process

        self._finished = True

    def is_ready(self):
        return self._finished

    def save_instance_state(self, bundle, exec_engine):
        super(WaitOnProcess, self).save_instance_state(bundle, exec_engine)
        bundle[self.WAIT_ON_PID] = exec_engine.get_pid(self._process)


class WaitOnProcessOutput(WaitOn, ProcessListener):
    WAIT_ON_PID = 'pid'
    OUTPUT_PORT = 'output_port'

    @classmethod
    def create_from(cls, bundle, exec_engine):
        return cls(bundle[cls.CALLBACK_NAME],
                   exec_engine.get_process(bundle[cls.WAIT_ON_PID]),
                   bundle[cls.OUTPUT_PORT])

    def __init__(self, callback_name, process, output_port):
        super(WaitOnProcessOutput, self).__init__(callback_name)
        self._process = process
        self._output_port = output_port
        self._finished = False
        process.add_process_listener(self)

    def on_output_emitted(self, process, output_port, value, dynamic):
        assert process is self._process
        if output_port == self._output_port:
            self._finished = True

    def is_ready(self):
        return self._finished

    def save_instance_state(self, bundle, exec_engine):
        super(WaitOnProcessOutput, self).save_instance_state(bundle, exec_engine)
        bundle[self.WAIT_ON_PID] = exec_engine.get_pid(self._process)
        bundle[self.OUTPUT_PORT] = self._output_port
