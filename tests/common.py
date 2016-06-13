

from plum.process import Process, ProcessListener
from plum.util import override
from plum.wait_ons import checkpoint


class DummyProcess(Process):
    @override
    def _run(self):
        pass


class ProcessEventsTester(Process):
    EVENTS = ["create", "recreate", "start", "continue", "exception","finish",
              "emitted", "wait", "stop", "destroy", ]

    called_events = []

    @classmethod
    def _define(cls, spec):
        spec.dynamic_output()

    @classmethod
    def called(cls, event):
        assert event in cls.EVENTS
        cls.called_events.append(event)

    def __init__(self):
        super(ProcessEventsTester, self).__init__()
        self._emitted = False
        self.__class__.called_events = []

    @override
    def on_create(self, pid, inputs=None):
        super(ProcessEventsTester, self).on_create(pid, inputs)
        self.called('create')

    @override
    def on_recreate(self, pid, saved_instance_state):
        super(ProcessEventsTester, self).on_recreate(pid, saved_instance_state)
        self.called('recreate')

    @override
    def on_start(self, exec_engine):
        super(ProcessEventsTester, self).on_start(exec_engine)
        self.called('start')

    @override
    def _on_output_emitted(self, output_port, value, dynamic):
        super(ProcessEventsTester, self)._on_output_emitted(
            output_port, value, dynamic)
        self.called('emitted')

    @override
    def on_wait(self, wait_on):
        super(ProcessEventsTester, self).on_wait(wait_on)
        self.called('wait')

    @override
    def on_continue(self, wait_on):
        super(ProcessEventsTester, self).on_continue(wait_on)
        self.called('continue')

    @override
    def on_fail(self, exception):
        super(ProcessEventsTester, self).on_fail(exception)
        self.called('exception')

    @override
    def on_finish(self, retval):
        super(ProcessEventsTester, self).on_finish(retval)
        self.called('finish')

    @override
    def on_stop(self):
        super(ProcessEventsTester, self).on_stop()
        self.called('stop')

    @override
    def on_destroy(self):
        super(ProcessEventsTester, self).on_destroy()
        self.called('destroy')

    @override
    def _run(self):
        self.out("test", 5)


class CheckpointProcess(ProcessEventsTester):
    @override
    def _run(self):
        self.out("test", 5)
        return checkpoint(self.finish)

    def finish(self, wait_on):
        pass


class ExceptionProcess(ProcessEventsTester):
    @override
    def _run(self):
        self.out("test", 5)
        raise RuntimeError("Great scott!")


class CheckpointThenExceptionProcess(CheckpointProcess):
    @override
    def finish(self, wait_on):
        raise RuntimeError("Great scott!")


class ProcessListenerTester(ProcessListener):
    def __init__(self):
        self.create = False
        self.start = False
        self.continue_ = False
        self.finish = False
        self.emitted = False
        self.stop = False
        self.destroy = False

    @override
    def on_create(self, pid, inputs=None):
        self.create = True

    @override
    def on_process_start(self, process):
        assert isinstance(process, Process)
        self.start = True

    @override
    def on_output_emitted(self, process, output_port, value, dynamic):
        assert isinstance(process, Process)
        self.emitted = True

    @override
    def on_process_wait(self, process, wait_on):
        assert isinstance(process, Process)
        self.wait = True

    @override
    def on_process_continue(self, process, wait_on):
        assert isinstance(process, Process)
        self.continue_ = True

    @override
    def on_process_finish(self, process, retval):
        assert isinstance(process, Process)
        self.finish = True

    @override
    def on_process_stop(self, process):
        assert isinstance(process, Process)
        self.stop = True

    @override
    def on_process_destroy(self, process):
        assert isinstance(process, Process)
        self.destroy = True
