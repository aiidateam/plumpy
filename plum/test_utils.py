

from plum.process import Process, ProcessListener
from plum.util import override
from plum.wait_ons import checkpoint


class DummyProcess(Process):
    @override
    def _run(self):
        pass


class EventsTesterMixin(object):
    EVENTS = ["create", "recreate", "start", "restart", "continue", "exception",
              "finish", "emitted", "wait", "stop", "destroy", ]

    called_events = []

    @classmethod
    def called(cls, event):
        assert event in cls.EVENTS
        cls.called_events.append(event)

    def __init__(self):
        assert isinstance(self, Process),\
            "Mixin has to be used with a type derived from a Process"
        self.__class__.called_events = []

    @override
    def on_create(self, pid, inputs=None):
        super(EventsTesterMixin, self).on_create(pid, inputs)
        self.called('create')

    @override
    def on_recreate(self, pid, saved_instance_state):
        super(EventsTesterMixin, self).on_recreate(pid, saved_instance_state)
        self.called('recreate')

    @override
    def on_start(self):
        super(EventsTesterMixin, self).on_start()
        self.called('start')

    @override
    def on_restart(self):
        super(EventsTesterMixin, self).on_restart()
        self.called('restart')

    @override
    def _on_output_emitted(self, output_port, value, dynamic):
        super(EventsTesterMixin, self)._on_output_emitted(
            output_port, value, dynamic)
        self.called('emitted')

    @override
    def on_wait(self, wait_on):
        super(EventsTesterMixin, self).on_wait(wait_on)
        self.called('wait')

    @override
    def on_continue(self, wait_on):
        super(EventsTesterMixin, self).on_continue(wait_on)
        self.called('continue')

    @override
    def on_fail(self, exception):
        super(EventsTesterMixin, self).on_fail(exception)
        self.called('exception')

    @override
    def on_finish(self, retval):
        super(EventsTesterMixin, self).on_finish(retval)
        self.called('finish')

    @override
    def on_stop(self):
        super(EventsTesterMixin, self).on_stop()
        self.called('stop')

    @override
    def on_destroy(self):
        super(EventsTesterMixin, self).on_destroy()
        self.called('destroy')



class ProcessEventsTester(EventsTesterMixin, Process):
    @classmethod
    def _define(cls, spec):
        spec.dynamic_output()

    def __init__(self):
        Process.__init__(self)

    @override
    def _run(self):
        self.out("test", 5)


class CheckpointProcess(ProcessEventsTester):
    @override
    def on_create(self, pid, inputs=None):
        super(CheckpointProcess, self).on_create(pid, inputs)
        self._last_checkpoint = None

    @override
    def _run(self):
        self.out("test", 5)
        cp = checkpoint(self.middle_step)
        self._last_checkpoint = cp
        return cp

    def middle_step(self, wait_on):
        assert wait_on is self._last_checkpoint
        cp = checkpoint(self.finish)
        self._last_checkpoint = cp
        return cp

    def finish(self, wait_on):
        assert wait_on is self._last_checkpoint


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
