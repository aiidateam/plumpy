
from collections import namedtuple

from plum.persistence.bundle import Bundle
from plum.process import Process, ProcessState
from plum.process_listener import ProcessListener
from plum.util import override
from plum.wait import WaitOn
from plum.wait_ons import Checkpoint
from plum.waiting_process import WaitingProcess, WaitListener


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


class KeyboardInterruptProc(Process):
    @override
    def _run(self):
        raise KeyboardInterrupt()


class ProcessWithCheckpoint(WaitingProcess):
    @override
    def _run(self):
        return Checkpoint(), self.finish.__name__

    def finish(self, wait_on):
        pass


class WaitForSignal(WaitOn):
    def __init__(self):
        super(WaitForSignal, self).__init__()

    def signal(self):
        self.done(True)


class WaitForSignalProcess(WaitingProcess):
    @override
    def _run(self):
        self._signal = WaitForSignal()
        return self._signal, self.finish.__name__

    def finish(self, wait_on):
        pass

    def signal(self):
        self._signal.signal()


class EventsTesterMixin(object):
    EVENTS = ["create", "run", "continue", "finish", "emitted", "wait",
              "stop", "destroy"]

    called_events = []

    @classmethod
    def called(cls, event):
        assert event in cls.EVENTS
        cls.called_events.append(event)

    def __init__(self):
        assert isinstance(self, Process),\
            "Mixin has to be used with a type derived from a Process"
        super(EventsTesterMixin, self).__init__()
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


class ProcessEventsTester(EventsTesterMixin, WaitingProcess):
    @classmethod
    def _define(cls, spec):
        super(ProcessEventsTester, cls)._define(spec)
        spec.dynamic_output()

    def __init__(self):
        #Process.__init__(self)
        super(ProcessEventsTester, self).__init__()

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
        return Checkpoint(), self.middle_step.__name__

    def middle_step(self, wait_on):
        return Checkpoint(), self.finish.__name__

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
        self.stopped = False

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
    def on_process_stopped(self, process):
        assert isinstance(process, Process)
        self.stopped = True


class Saver(object):
    def __init__(self):
        self.snapshots = []
        self.outputs = []

    def _save(self, p):
        b = Bundle()
        p.save_instance_state(b)
        self.snapshots.append((p.state, b))
        self.outputs.append(p.outputs.copy())


class ProcessSaver(ProcessListener, Saver):
    """
    Save the instance state of a process each time it is about to enter a new
    state
    """
    def __init__(self, p):
        ProcessListener.__init__(self)
        Saver.__init__(self)
        p.add_process_listener(self)

    @override
    def on_process_start(self, process):
        self._save(process)

    @override
    def on_process_run(self, process):
        self._save(process)

    @override
    def on_process_finish(self, process):
        self._save(process)

    @override
    def on_process_stop(self, process):
        self._save(process)

    @override
    def on_process_stopped(self, process):
        self._save(process)


class WaitSaver(WaitListener, Saver):
    """
    Save the instance state of a process when it waits
    """
    def __init__(self, p):
        WaitListener.__init__(self)
        Saver.__init__(self)
        p.add_wait_listener(self)

    @override
    def on_process_wait(self, p, w):
        self._save(p)


class ProcessWaitSaver(ProcessSaver, WaitSaver):
    def __init__(self, proc):
        ProcessSaver.__init__(self, proc)
        WaitSaver.__init__(self, proc)


# All the Processes that can be used
TEST_PROCESSES = [DummyProcess, DummyProcessWithOutput]

TEST_WAITING_PROCESSES = [ProcessWithCheckpoint, TwoCheckpointProcess,
                          ExceptionProcess, ProcessEventsTester,
                          TwoCheckpointThenExceptionProcess]


def check_process_against_snapshots(proc_class, snapshots):
    """
    Take the series of snapshots from a Process that executed and run it
    forward from each one.  Check that the subsequent snapshots match.
    This will only check up to the STARTED state because from that state back
    they should of course differ.

    Return True if they match, False otherwise.

    :param proc_class: The process class to check
    :type proc_class: :class:`Process`
    :param snapshots: The snapshots taken from from an execution of that
      process
    :return: True if snapshots match False otherwise
    :rtype: bool
    """
    for i, info in zip(range(0, len(snapshots)), snapshots):
        loaded = proc_class.create_from(info[1])
        ps = ProcessSaver(loaded)
        try:
            loaded.start()
        except BaseException:
            pass

        # Now check going backwards until running that the saved states match
        j = 1
        while True:
            if j >= min(len(snapshots), len(ps.snapshots)) or \
                            ps.snapshots[-j][0] is ProcessState.STARTED:
                break

            if snapshots[-j] != ps.snapshots[-j]:
                return False
            j += 1
        return True