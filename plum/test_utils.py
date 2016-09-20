
from collections import namedtuple

from plum.persistence.bundle import Bundle
from plum.process import Process
from plum.process_listener import ProcessListener
from plum.util import override
from plum.wait import WaitOn
from plum.wait_ons import checkpoint


Snapshot = namedtuple('Snapshot', ['state', 'bundle', 'outputs'])


def create_snapshot(proc):
    b = Bundle()
    proc.save_instance_state(b)
    return Snapshot(proc.state, b, proc.outputs.copy())


class DummyProcess(Process):
    """
    Process with no inputs or ouputs and does nothing when ran.
    """

    @override
    def _run(self):
        pass


class DummyProcessWithOutput(Process):
    @classmethod
    def _define(cls, spec):
        super(DummyProcessWithOutput, cls)._define(spec)

        spec.dynamic_input()
        spec.dynamic_output()

    def _run(self, **kwargs):
        self.out("default", 5)


class ProcessWithCheckpoint(Process):
    @override
    def _run(self):
        return checkpoint(self.finish)

    def finish(self, wait_on):
        pass


class WaitForSignal(WaitOn):
    def __init__(self, callback_name):
        super(WaitForSignal, self).__init__(callback_name)
        self._ready = False

    def signal(self):
        self._ready = True

    @override
    def is_ready(self):
        return self._ready


class WaitForSignalProcess(Process):
    @override
    def _run(self):
        self._signal = WaitForSignal('finish')
        return self._signal

    def finish(self, wait_on):
        pass

    def signal(self):
        self._signal.signal()


class KeyboardInterruptProc(Process):
    @override
    def _run(self):
        raise KeyboardInterrupt()


class EventsTesterMixin(object):
    EVENTS = ["create", "run", "continue", "finish", "emitted", "wait",
              "stop", "destroy", ]

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
    def on_create(self, pid, inputs, saved_instance_state):
        super(EventsTesterMixin, self).on_create(
            pid, inputs, saved_instance_state)
        self.called('create')

    @override
    def on_run(self):
        super(EventsTesterMixin, self).on_run()
        self.called('run')

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
    def on_finish(self):
        super(EventsTesterMixin, self).on_finish()
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
        super(ProcessEventsTester, cls)._define(spec)
        spec.dynamic_output()

    def __init__(self):
        Process.__init__(self)

    @override
    def _run(self):
        self.out("test", 5)


class TwoCheckpointProcess(ProcessEventsTester):
    @override
    def on_create(self, pid, inputs, saved_instance_state):
        super(TwoCheckpointProcess, self).on_create(
            pid, inputs, saved_instance_state)
        self._last_checkpoint = None

    @override
    def _run(self):
        self.out("test", 5)
        return checkpoint(self.middle_step)

    def middle_step(self, wait_on):
        return checkpoint(self.finish)

    def finish(self, wait_on):
        pass


class ExceptionProcess(ProcessEventsTester):
    @override
    def _run(self):
        self.out("test", 5)
        raise RuntimeError("Great scott!")


class TwoCheckpointThenExceptionProcess(TwoCheckpointProcess):
    @override
    def finish(self, wait_on):
        raise RuntimeError("Great scott!")


class ProcessListenerTester(ProcessListener):
    def __init__(self):
        self.create = False
        self.run = False
        self.continue_ = False
        self.finish = False
        self.emitted = False
        self.stop = False
        self.destroy = False

    @override
    def on_process_run(self, process):
        assert isinstance(process, Process)
        self.run = True

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
    def on_process_finish(self, process):
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


# All the Processes that can be used
TEST_PROCESSES = [DummyProcess, DummyProcessWithOutput, ProcessEventsTester,
                  ProcessWithCheckpoint, TwoCheckpointProcess,
                  ExceptionProcess, TwoCheckpointThenExceptionProcess]