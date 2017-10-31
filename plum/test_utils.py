from collections import namedtuple

import apricotpy
from plum.process import Process
from plum.process_listener import ProcessListener
from plum.utils import override
from plum.wait_ons import Checkpoint, Barrier

Snapshot = namedtuple('Snapshot', ['state', 'bundle', 'outputs'])


def create_snapshot(proc):
    b = apricotpy.Bundle()
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
    def define(cls, spec):
        super(DummyProcessWithOutput, cls).define(spec)

        spec.dynamic_input()
        spec.dynamic_output()

    def _run(self, **kwargs):
        self.out("default", 5)


class KeyboardInterruptProc(Process):
    @override
    def _run(self):
        raise KeyboardInterrupt()


class ProcessWithCheckpoint(Process):
    @override
    def _run(self):
        return self.loop().create(Checkpoint), self.finish

    def finish(self):
        pass


class WaitForSignalProcess(Process):
    BARRIER = 'barrier'

    @override
    def _run(self):
        return self.loop().create(Barrier), self.finish

    def finish(self):
        pass

    def continue_(self):
        """
        Can only be called when in the WAITING state
        """
        self.get_waiting_on().open()


class EventsTesterMixin(object):
    EVENTS = ["create", "run", "finish", "emitted", "wait", "resume", "stop", "terminate"]

    called_events = []

    @classmethod
    def called(cls, event):
        assert event in cls.EVENTS, "Unknown event '{}'".format(event)
        cls.called_events.append(event)

    def __init__(self, inputs=None, pid=None, logger=None):
        assert isinstance(self, Process), \
            "Mixin has to be used with a type derived from a Process"
        super(EventsTesterMixin, self).__init__(inputs, pid, logger)
        self.__class__.called_events = []

    @override
    def on_create(self):
        super(EventsTesterMixin, self).on_create()
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
    def on_resume(self):
        super(EventsTesterMixin, self).on_resume()
        self.called('resume')

    @override
    def on_finish(self):
        super(EventsTesterMixin, self).on_finish()
        self.called('finish')

    @override
    def on_stop(self):
        super(EventsTesterMixin, self).on_stop()
        self.called('stop')

    @override
    def on_terminate(self):
        super(EventsTesterMixin, self).on_terminate()
        self.called('terminate')


class ProcessEventsTester(EventsTesterMixin, Process):
    @classmethod
    def define(cls, spec):
        super(ProcessEventsTester, cls).define(spec)
        spec.dynamic_output()

    @override
    def _run(self):
        self.out("test", 5)


class TwoCheckpoint(ProcessEventsTester):
    def __init__(self, inputs=None, pid=None, logger=None):
        super(TwoCheckpoint, self).__init__(inputs, pid, logger)
        self._last_checkpoint = None

    @override
    def _run(self):
        self.out("test", 5)
        return self.loop().create(Checkpoint), self.middle_step

    def middle_step(self, wait_on):
        return self.loop().create(Checkpoint), self.finish

    def finish(self, wait_on):
        pass


class TwoCheckpointNoFinish(ProcessEventsTester):
    @override
    def _run(self):
        self.out("test", 5)
        return self.loop().create(Checkpoint), self.middle_step

    def middle_step(self, wait_on):
        return self.loop().create(Checkpoint), None


class ExceptionProcess(ProcessEventsTester):
    @override
    def _run(self):
        self.out("test", 5)
        raise RuntimeError("Great scott!")


class TwoCheckpointThenException(TwoCheckpoint):
    @override
    def finish(self, wait_on):
        raise RuntimeError("Great scott!")


class ProcessListenerTester(ProcessListener):
    def __init__(self):
        self.start = False
        self.run = False
        self.continue_ = False
        self.emitted = False
        self.finish = False
        self.stop = False
        self.terminate = False

    @override
    def on_process_start(self, process):
        self.start = True

    @override
    def on_process_run(self, process):
        self.run = True

    @override
    def on_output_emitted(self, process, output_port, value, dynamic):
        self.emitted = True

    @override
    def on_process_wait(self, process):
        self.wait = True

    @override
    def on_process_continue(self, process, wait_on):
        self.continue_ = True

    @override
    def on_process_finish(self, process):
        self.finish = True

    @override
    def on_process_stop(self, process):
        self.stop = True

    @override
    def on_process_terminate(self, process):
        self.terminate = True


class Saver(object):
    def __init__(self):
        self.snapshots = []
        self.outputs = []

    def _save(self, p):
        b = apricotpy.persistable.Bundle(p)

        self.snapshots.append((p.state, b))
        self.outputs.append(p.outputs.copy())


class ProcessSaver(ProcessListener, Saver):
    """
    Save the instance state of a process each time it is about to enter a new state
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
    def on_process_wait(self, process):
        self._save(process)

    @override
    def on_process_finish(self, process):
        self._save(process)

    @override
    def on_process_stop(self, process):
        self._save(process)


# All the Processes that can be used
TEST_PROCESSES = [DummyProcess, DummyProcessWithOutput]

TEST_WAITING_PROCESSES = [
    ProcessWithCheckpoint,
    TwoCheckpoint,
    TwoCheckpointNoFinish,
    ExceptionProcess,
    ProcessEventsTester,
    TwoCheckpointThenException
]


def check_process_against_snapshots(loop, proc_class, snapshots):
    """
    Take the series of snapshots from a Process that executed and run it
    forward from each one.  Check that the subsequent snapshots match.
    This will only check up to the STARTED state because from that state back
    they should of course differ.

    Return True if they match, False otherwise.

    :param loop: The event loop
    :type loop: :class:`plum.loop.event_loop.AbstractEventLoop`
    :param proc_class: The process class to check
    :type proc_class: :class:`Process`
    :param snapshots: The snapshots taken from from an execution of that
      process
    :return: True if snapshots match False otherwise
    :rtype: bool
    """
    for i, info in zip(range(0, len(snapshots)), snapshots):

        bundle = info[1]
        loaded = bundle.unbundle(loop)
        ps = ProcessSaver(loaded)
        try:
            loop.run_until_complete(loaded)
        except BaseException:
            pass

        # Now check going backwards until running that the saved states match
        j = 1
        while True:
            if j >= min(len(snapshots), len(ps.snapshots)):
                break

            if snapshots[-j] != ps.snapshots[-j]:
                return False
            j += 1

        return True
