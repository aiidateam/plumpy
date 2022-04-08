# -*- coding: utf-8 -*-
"""Utilities for tests"""
import asyncio
import collections
from collections.abc import Mapping
import unittest

import kiwipy.rmq
import shortuuid

import plumpy
from plumpy import persistence, process_states, processes, utils

Snapshot = collections.namedtuple('Snapshot', ['state', 'bundle', 'outputs'])


class TestCase(unittest.TestCase):
    pass


class DummyProcess(processes.Process):
    """
    Process with no inputs or outputs and does nothing when ran.
    """

    EXPECTED_STATE_SEQUENCE = [
        process_states.ProcessState.CREATED, process_states.ProcessState.RUNNING, process_states.ProcessState.FINISHED
    ]

    EXPECTED_OUTPUTS = {}


class DummyProcessWithOutput(processes.Process):
    EXPECTED_OUTPUTS = {'default': 5}

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.inputs.dynamic = True
        spec.outputs.dynamic = True
        spec.output('default', valid_type=int)

    def run(self, **kwargs):
        self.out('default', 5)


class DummyProcessWithDynamicOutput(processes.Process):
    EXPECTED_OUTPUTS = {'default': 5}

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.inputs.dynamic = True
        spec.outputs.dynamic = True

    def run(self, **kwargs):
        self.out('default', 5)


class KeyboardInterruptProc(processes.Process):

    @utils.override
    def run(self):
        raise KeyboardInterrupt()


class ProcessWithCheckpoint(processes.Process):

    @utils.override
    def run(self):
        return process_states.Continue(self.last_step)

    def last_step(self):
        pass


class WaitForSignalProcess(processes.Process):

    @utils.override
    def run(self):
        return process_states.Wait(self.last_step)

    def last_step(self):
        pass


class KillProcess(processes.Process):

    @utils.override
    def run(self):
        return process_states.Kill('killed')


class MissingOutputProcess(processes.Process):
    """ A process that does not generate a required output """

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.output('default', required=True)


class NewLoopProcess(processes.Process):

    def __init__(self, *args, **kwargs):
        kwargs['loop'] = plumpy.new_event_loop()
        super().__init__(*args, **kwargs)


class EventsTesterMixin:
    EVENTS = ('create', 'run', 'finish', 'emitted', 'wait', 'resume', 'stop', 'terminate')

    called_events = []

    @classmethod
    def called(cls, event):
        assert event in cls.EVENTS, f"Unknown event '{event}'"
        cls.called_events.append(event)

    def __init__(self, *args, **kwargs):
        assert isinstance(self, processes.Process), \
            'Mixin has to be used with a type derived from a Process'
        super().__init__(*args, **kwargs)
        self.__class__.called_events = []

    @utils.override
    def on_create(self):
        super().on_create()
        self.called('create')

    @utils.override
    def on_run(self):
        super().on_run()
        self.called('run')

    @utils.override
    def _on_output_emitted(self, output_port, value, dynamic):
        super()._on_output_emitted(output_port, value, dynamic)
        self.called('emitted')

    @utils.override
    def on_wait(self, wait_on):
        super().on_wait(wait_on)
        self.called('wait')

    @utils.override
    def on_resume(self):
        super().on_resume()
        self.called('resume')

    @utils.override
    def on_finish(self, result, successful):
        super().on_finish(result, successful)
        self.called('finish')

    @utils.override
    def on_stop(self):
        super().on_stop()
        self.called('stop')

    @utils.override
    def on_terminate(self):
        super().on_terminate()
        self.called('terminate')


class ProcessEventsTester(EventsTesterMixin, processes.Process):

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.outputs.dynamic = True

    def run(self):
        self.out('test', 5)


class ThreeSteps(ProcessEventsTester):
    EXPECTED_OUTPUTS = {'test': 5}

    _last_checkpoint = None

    @utils.override
    def run(self):
        self.out('test', 5)
        return process_states.Continue(self.middle_step)

    def middle_step(self,):
        return process_states.Continue(self.last_step)

    def last_step(self):
        pass


class TwoCheckpointNoFinish(ProcessEventsTester):

    def run(self):
        self.out('test', 5)
        return process_states.Continue(self.middle_step)

    def middle_step(self):
        pass


class ExceptionProcess(ProcessEventsTester):

    def run(self):
        self.out('test', 5)
        raise RuntimeError('Great scott!')


class ThreeStepsThenException(ThreeSteps):

    @utils.override
    def last_step(self):
        raise RuntimeError('Great scott!')


class ProcessListenerTester(plumpy.ProcessListener):

    def __init__(self, process, expected_events):
        process.add_process_listener(self)
        self.expected_events = set(expected_events)
        self.called = set()

    def on_process_created(self, process):
        self.called.add('created')

    def on_process_running(self, process):
        self.called.add('running')

    def on_process_waiting(self, process):
        self.called.add('waiting')

    def on_process_paused(self, process):
        self.called.add('paused')

    def on_output_emitted(self, process, output_port, value, dynamic):
        self.called.add('output_emitted')

    def on_process_finished(self, process, outputs):
        self.called.add('finished')

    def on_process_excepted(self, process, reason):
        self.called.add('excepted')

    def on_process_killed(self, process, msg):
        self.called.add('killed')


class Saver:

    def __init__(self):
        self.snapshots = []
        self.outputs = []

    def _save(self, p):
        b = persistence.Bundle(p)
        self.snapshots.append(b)
        self.outputs.append(p.outputs.copy())


class ProcessSaver(plumpy.ProcessListener, Saver):
    """
    Save the instance state of a process each time it is about to enter a new state
    """

    def __init__(self, proc):
        plumpy.ProcessListener.__init__(self)
        Saver.__init__(self)
        self.process = proc
        proc.add_process_listener(self)

    def capture(self):
        self._save(self.process)
        if not self.process.has_terminated():
            try:
                self.process.execute()
            except Exception:
                pass

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

    @utils.override
    def on_process_excepted(self, process, reason):
        self._save(process)

    @utils.override
    def on_process_killed(self, process, msg):
        self._save(process)


# All the Processes that can be used
TEST_PROCESSES = [DummyProcess, DummyProcessWithOutput, DummyProcessWithDynamicOutput, ThreeSteps]

TEST_WAITING_PROCESSES = [
    ProcessWithCheckpoint, TwoCheckpointNoFinish, ExceptionProcess, ProcessEventsTester, ThreeStepsThenException
]

TEST_EXCEPTION_PROCESSES = [ExceptionProcess, ThreeStepsThenException, MissingOutputProcess]


def check_process_against_snapshots(loop, proc_class, snapshots):
    """
    Take the series of snapshots from a Process that executed and run it
    forward from each one.  Check that the subsequent snapshots match.
    This will only check up to the STARTED state because from that state back
    they should of course differ.

    Return True if they match, False otherwise.

    :param loop: The event loop
    :param proc_class: The process class to check
    :type proc_class: :class:`Process`
    :param snapshots: The snapshots taken from from an execution of that
      process
    :return: True if snapshots match False otherwise
    :rtype: bool
    """
    for i, bundle in zip(list(range(0, len(snapshots))), snapshots):
        loaded = bundle.unbundle(plumpy.LoadSaveContext(loop=loop))
        saver = ProcessSaver(loaded)
        saver.capture()

        # Now check going backwards until running that the saved states match
        j = 1
        while True:
            if j >= min(len(snapshots), len(saver.snapshots)):
                break

            compare_dictionaries(
                snapshots[-j], saver.snapshots[-j], snapshots[-j], saver.snapshots[-j], exclude={'exception'}
            )
            j += 1

    return True


def compare_dictionaries(bundle1, bundle2, dict1, dict2, exclude=None):
    keys = set(dict1.keys()) & set(dict2.keys())
    if exclude is not None:
        keys -= exclude

    for key in keys:
        if key not in dict1:
            raise ValueError(f"Key '{key}' in dict 1 but not 2")

        if key not in dict2:
            raise ValueError(f"Key '{key}' in dict 2 but not 1")

        v1 = dict1[key]
        v2 = dict2[key]

        compare_value(bundle1, bundle2, v1, v2, exclude)


def compare_value(bundle1, bundle2, v1, v2, exclude=None):
    if isinstance(v1, Mapping) and isinstance(v2, Mapping):
        compare_dictionaries(bundle1, bundle2, v1, v2, exclude)
    elif isinstance(v1, list) and isinstance(v2, list):
        for vv1, vv2 in zip(v1, v2):
            compare_value(bundle1, bundle2, vv1, vv2, exclude)
    else:
        if v1 != v2:
            raise ValueError(f'Dict values mismatch for :\n{v1} != {v2}')


class TestPersister(persistence.Persister):
    """
    Test persister, just creates the bundle, noting else
    """

    def save_checkpoint(self, process, tag=None):
        """ Create the checkpoint bundle """
        persistence.Bundle(process)

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


def run_until_waiting(proc):
    """ Set up a future that will be resolved on entering the WAITING state """
    from plumpy import ProcessState

    listener = plumpy.ProcessListener()
    in_waiting = plumpy.Future()

    if proc.state == ProcessState.WAITING:
        in_waiting.set_result(True)
    else:

        def on_waiting(_waiting_proc):
            in_waiting.set_result(True)
            proc.remove_process_listener(listener)

        listener.on_process_waiting = on_waiting
        proc.add_process_listener(listener)

    return in_waiting


def run_until_paused(proc):
    """ Set up a future that will be resolved when the process is paused """

    listener = plumpy.ProcessListener()
    paused = plumpy.Future()

    if proc.paused:
        paused.set_result(True)
    else:

        def on_paused(_paused_proc):
            paused.set_result(True)
            proc.remove_process_listener(listener)

        listener.on_process_paused = on_paused
        proc.add_process_listener(listener)

    return paused


async def wait_util(condition, sleep_interval=0.1):
    """Given a condition function, keep polling until it returns True"""
    while not condition():
        await asyncio.sleep(sleep_interval)
