

from plum.process import Process, ProcessListener
from plum.util import override
from plum.wait_ons import Checkpoint


class DummyProcess(Process):
    @override
    def _run(self):
        pass


class ProcessEventsTester(Process):
    EVENTS = ["create", "start", "continue_", "finish", "emitted", "stop",
              "destroy",]

    @staticmethod
    def _define(spec):
        spec.optional_output("create")
        spec.optional_output("start")
        spec.optional_output("continue_")
        spec.optional_output("finish")
        spec.optional_output("emitted")
        spec.optional_output("stop")
        spec.optional_output("destroy")

    def __init__(self):
        super(ProcessEventsTester, self).__init__()
        self._emitted = False

    @override
    def on_create(self, pid, saved_instance_state=None):
        super(ProcessEventsTester, self).on_create(pid, saved_instance_state)
        self.out("create", True)

    @override
    def on_start(self, inputs, exec_engine):
        super(ProcessEventsTester, self).on_start(inputs, exec_engine)
        self.out("start", True)

    @override
    def _on_output_emitted(self, output_port, value, dynamic):
        super(ProcessEventsTester, self)._on_output_emitted(
            output_port, value, dynamic)
        if not self._emitted:
            self._emitted = True
            self.out("emitted", True)

    @override
    def on_wait(self, wait_on):
        super(ProcessEventsTester, self).on_wait(wait_on)
        self.out("wait", True)

    @override
    def on_continue(self, wait_on):
        super(ProcessEventsTester, self).on_continue(wait_on)
        self.out("continue_", True)

    @override
    def on_finish(self, retval):
        super(ProcessEventsTester, self).on_finish(retval)
        self.out("finish", True)

    @override
    def on_stop(self):
        super(ProcessEventsTester, self).on_stop()
        self.out("stop", True)

    @override
    def on_destroy(self):
        super(ProcessEventsTester, self).on_destroy()
        self.out("destroy", True)

    @override
    def _run(self):
        return Checkpoint(self.finish.__name__)

    def finish(self, wait_on):
        pass


class ProcessListenerTester(ProcessListener):
    def __init__(self):
        self.start = False
        self.continue_ = False
        self.finish = False
        self.emitted = False
        self.stop = False
        self.destroy = False

    @override
    def on_process_start(self, process, inputs):
        self.start = True

    @override
    def on_output_emitted(self, process, output_port, value, dynamic):
        self.emitted = True

    @override
    def on_process_wait(self, process, wait_on):
        self.wait = True

    @override
    def on_process_continue(self, process, wait_on):
        self.continue_ = True

    @override
    def on_process_finish(self, process, retval):
        self.finish = True

    @override
    def on_process_stop(self, process):
        self.stop = True

    @override
    def on_process_destroy(self, process):
        self.destroy = True
