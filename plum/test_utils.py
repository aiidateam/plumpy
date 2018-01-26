import collections
from collections import namedtuple

import plum
from plum.process_listener import ProcessListener
from . import process
from . import persisters
from . import utils

Snapshot = namedtuple('Snapshot', ['state', 'bundle', 'outputs'])


class DummyProcess(process.Process):
    """
    Process with no inputs or outputs and does nothing when ran.
    """

    EXPECTED_STATE_SEQUENCE = [
        process.ProcessState.CREATED,
        process.ProcessState.RUNNING,
        process.ProcessState.FINISHED]

    @utils.override
    def _run(self):
        pass


class DummyProcessWithOutput(process.Process):
    EXPECTED_OUTPUTS = {'default': 5}

    @classmethod
    def define(cls, spec):
        super(DummyProcessWithOutput, cls).define(spec)
        spec.dynamic_input()
        spec.dynamic_output()

    def _run(self, **kwargs):
        self.out("default", 5)
        return self.outputs


class KeyboardInterruptProc(process.Process):
    @utils.override
    def _run(self):
        raise KeyboardInterrupt()


class ProcessWithCheckpoint(process.Process):
    @utils.override
    def _run(self):
        return process.Continue(self.last_step)

    def last_step(self):
        pass


class WaitForSignalProcess(process.Process):
    @utils.override
    def _run(self):
        return process.Wait(self.last_step)

    def last_step(self):
        pass


class EventsTesterMixin(object):
    EVENTS = ("create", "run", "finish", "emitted", "wait", "resume", "stop", "terminate")

    called_events = []

    @classmethod
    def called(cls, event):
        assert event in cls.EVENTS, "Unknown event '{}'".format(event)
        cls.called_events.append(event)

    def __init__(self, *args, **kwargs):
        assert isinstance(self, process.Process), \
            "Mixin has to be used with a type derived from a Process"
        super(EventsTesterMixin, self).__init__(*args, **kwargs)
        self.__class__.called_events = []

    @utils.override
    def on_create(self):
        super(EventsTesterMixin, self).on_create()
        self.called('create')

    @utils.override
    def on_run(self):
        super(EventsTesterMixin, self).on_run()
        self.called('run')

    @utils.override
    def _on_output_emitted(self, output_port, value, dynamic):
        super(EventsTesterMixin, self)._on_output_emitted(
            output_port, value, dynamic)
        self.called('emitted')

    @utils.override
    def on_wait(self, wait_on):
        super(EventsTesterMixin, self).on_wait(wait_on)
        self.called('wait')

    @utils.override
    def on_resume(self):
        super(EventsTesterMixin, self).on_resume()
        self.called('resume')

    @utils.override
    def on_finish(self, result):
        super(EventsTesterMixin, self).on_finish(result)
        self.called('finish')

    @utils.override
    def on_stop(self):
        super(EventsTesterMixin, self).on_stop()
        self.called('stop')

    @utils.override
    def on_terminate(self):
        super(EventsTesterMixin, self).on_terminate()
        self.called('terminate')


class ProcessEventsTester(EventsTesterMixin, process.Process):
    @classmethod
    def define(cls, spec):
        super(ProcessEventsTester, cls).define(spec)
        spec.dynamic_output()

    @utils.override
    def _run(self):
        self.out("test", 5)


class ThreeSteps(ProcessEventsTester):
    _last_checkpoint = None

    @utils.override
    def _run(self):
        self.out("test", 5)
        return process.Continue(self.middle_step)

    def middle_step(self, ):
        return process.Continue(self.last_step)

    def last_step(self):
        pass


class TwoCheckpointNoFinish(ProcessEventsTester):
    @utils.override
    def _run(self):
        self.out("test", 5)
        return process.Continue(self.middle_step)

    def middle_step(self):
        pass


class ExceptionProcess(ProcessEventsTester):
    @utils.override
    def _run(self):
        self.out("test", 5)
        raise RuntimeError("Great scott!")


class ThreeStepsThenException(ThreeSteps):
    @utils.override
    def last_step(self):
        raise RuntimeError("Great scott!")


class ProcessListenerTester(ProcessListener):
    def __init__(self, process, expected_events, done_callback):
        process.add_process_listener(self)
        self.expected_events = set(expected_events)
        self._done_callback = done_callback
        self.called = set()

    def on_process_created(self, process):
        self.called.add('created')
        self._check_done()

    def on_process_running(self, process):
        self.called.add('running')
        self._check_done()

    def on_process_waiting(self, process, data):
        self.called.add('waiting')
        self._check_done()

    def on_process_paused(self, process):
        self.called.add('paused')
        self._check_done()

    def on_output_emitted(self, process, output_port, value, dynamic):
        self.called.add('output_emitted')
        self._check_done()

    def on_process_finished(self, process, outputs):
        self.called.add('finished')
        self._check_done()

    def on_process_failed(self, process, exc_info):
        self.called.add('failed')
        self._check_done()

    def on_process_cancelled(self, process, msg):
        self.called.add('cancelled')
        self._check_done()

    def _check_done(self):
        if self.called == self.expected_events:
            self._done_callback()


class Saver(object):
    def __init__(self):
        self.snapshots = []
        self.outputs = []

    def _save(self, p):
        b = persisters.Bundle(p)
        self.snapshots.append(b)
        self.outputs.append(p.outputs.copy())


class ProcessSaver(ProcessListener, Saver):
    """
    Save the instance state of a process each time it is about to enter a new state
    """

    def __init__(self, proc):
        ProcessListener.__init__(self)
        Saver.__init__(self)
        self.process = proc
        proc.add_process_listener(self)
        self._future = plum.Future()

    def capture(self):
        self._save(self.process)
        if not self.process.done():
            self.process.play()
            self._future.add_done_callback(lambda _: self.process.loop().stop())
            self.process.loop().start()

    @utils.override
    def on_process_running(self, process):
        self._save(process)

    @utils.override
    def on_process_waiting(self, process, data):
        self._save(process)

    @utils.override
    def on_process_paused(self, process):
        self._save(process)

    # Terminal states:

    @utils.override
    def on_process_finished(self, process, outputs):
        self._save(process)
        self._future.set_result(True)

    @utils.override
    def on_process_failed(self, process, exc_info):
        self._save(process)
        self._future.set_result(True)

    @utils.override
    def on_process_cancelled(self, process, msg):
        self._save(process)
        self._future.set_result(True)


# All the Processes that can be used
TEST_PROCESSES = [
    DummyProcess,
    DummyProcessWithOutput,
    ThreeSteps]

TEST_WAITING_PROCESSES = [
    ProcessWithCheckpoint,
    TwoCheckpointNoFinish,
    ExceptionProcess,
    ProcessEventsTester,
    ThreeStepsThenException]

TEST_EXCEPTION_PROCESSES = [
    ExceptionProcess,
    ThreeStepsThenException
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
    for i, bundle in zip(range(0, len(snapshots)), snapshots):
        loaded = bundle.unbundle(loop=loop)
        saver = ProcessSaver(loaded)
        saver.capture()

        # Now check going backwards until running that the saved states match
        j = 1
        while True:
            if j >= min(len(snapshots), len(saver.snapshots)):
                break

            compare_dictionaries(
                snapshots[-j], saver.snapshots[-j],
                snapshots[-j], saver.snapshots[-j],
                exclude={
                    str(process.BundleKeys.LOOP_CALLBACK),
                    str(process.BundleKeys.AWAITING),
                    'CALLBACKS', 'DONE_CALLBACKS', 'SCHEDULED_CALLBACKS', 'PERSISTABLE_ID'
                })
            j += 1

    return True


def compare_dictionaries(bundle1, bundle2, dict1, dict2, exclude=None):
    keys = set(dict1.keys()) & set(dict2.keys())
    if exclude is not None:
        keys -= exclude

    for key in keys:
        if key not in dict1:
            raise ValueError("Key '{}' in dict 1 but not 2".format(key))

        if key not in dict2:
            raise ValueError("Key '{}' in dict 2 but not 1".format(key))

        v1 = dict1[key]
        v2 = dict2[key]

        compare_value(bundle1, bundle2, v1, v2, exclude)


def compare_value(bundle1, bundle2, v1, v2, exclude=None):
    if isinstance(v1, collections.Mapping) and isinstance(v2, collections.Mapping):
        compare_dictionaries(bundle1, bundle2, v1, v2, exclude)
    elif isinstance(v1, list) and isinstance(v2, list):
        for vv1, vv2 in zip(v1, v2):
            compare_value(bundle1, bundle2, vv1, vv2, exclude)
    else:
        if v1 != v2:
            raise ValueError(
                "Dict values mismatch for :\n{} != {}".format(v1, v2)
            )


class TestPersister(persisters.Persister):
    """
    Test persister, just creates the bundle, noting else
    """
    def save_checkpoint(self, process, tag=None):
        """ Create the checkpoint bundle """
        persisters.Bundle(process)

    def load_checkpoint(self, pid, tag=None):
        raise NotImplementedError

    def get_checkpoints(self):
        raise NotImplementedError

    def get_process_checkpoints(self, pid):
        raise NotImplementedError

    def delete_checkpoint(self, pid, tag=None):
        raise NotImplementedError

    def delete_process_checkpoints(self, pid):
        raise NotImplementedError
