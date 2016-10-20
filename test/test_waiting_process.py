from unittest import TestCase

import threading
from plum.persistence.bundle import Bundle
from plum.process import Process, ProcessState
from plum.process_monitor import MONITOR
from plum.test_utils import TwoCheckpointProcess, \
    DummyProcessWithOutput, TEST_WAITING_PROCESSES, WaitForSignalProcess
from plum.test_utils import ProcessListenerTester
from plum.util import override
from plum.test_utils import WaitSaver, ProcessWaitSaver
from plum.wait_ons import wait_until_destroyed, wait_until_state


class TestWaitingProcess(TestCase):
    def setUp(self):
        self.events_tester = ProcessListenerTester()
        self.proc = DummyProcessWithOutput()
        self.proc.add_process_listener(self.events_tester)

    def tearDown(self):
        self.proc.remove_process_listener(self.events_tester)

    def test_instance_state(self):
        proc = TwoCheckpointProcess.new_instance()
        wl = WaitSaver(proc)
        proc.start()

        for snapshot, outputs in zip(wl.snapshots, wl.outputs):
            state, bundle = snapshot
            self.assertEqual(
                outputs, bundle[Process.BundleKeys.OUTPUTS.value].get_dict())

    def test_saving_each_step(self):
        for ProcClass in TEST_WAITING_PROCESSES:
            proc = ProcClass.new_instance()
            saver = ProcessWaitSaver(proc)
            proc.start()

            self._check_process_against_snapshots(
                ProcClass, saver.snapshots)

    def test_fast_forward(self):
        import plum.knowledge_provider as knowledge_provider
        from plum.in_memory_database import InMemoryDatabase

        class FastForwarding(Process):
            @classmethod
            def _define(cls, spec):
                super(FastForwarding, cls)._define(spec)

                spec.input("a", required=True)
                spec.output("out")
                spec.deterministic()

            def __init__(self):
                super(FastForwarding, self).__init__()
                self.did_ff = False

            @override
            def fast_forward(self):
                super(FastForwarding, self).fast_forward()
                self.did_ff = True

            @override
            def _run(self, **kwargs):
                self.out("out", self.inputs.a)

        old_kp = knowledge_provider.get_global_provider()
        imdb = InMemoryDatabase(retain_inputs=True, retain_outputs=True)
        knowledge_provider.set_global_provider(imdb)

        for ProcClass in TEST_WAITING_PROCESSES:
            # Try running first time
            try:
                outputs = ProcClass.run()
            except BaseException:
                pass
            else:
                # Check that calling again doesn't mess with the process
                outputs2 = ProcClass.run()
                self.assertEqual(outputs, outputs2)

        ff_proc = FastForwarding.new_instance(inputs={'a': 5})
        ff_proc.start()
        outs1 = ff_proc.outputs
        self.assertFalse(ff_proc.did_ff)

        # Check the same inputs again
        ff_proc = FastForwarding.new_instance(inputs={'a': 5})
        ff_proc.start()
        outs2 = ff_proc.outputs
        self.assertTrue(ff_proc.did_ff)
        self.assertEqual(outs1, outs2)

        # Now check different inputs
        ff_proc = FastForwarding.new_instance(inputs={'a': 6})
        ff_proc.start()
        outs3 = ff_proc.outputs
        self.assertFalse(ff_proc.did_ff)
        self.assertNotEqual(outs1, outs3)

        knowledge_provider.set_global_provider(old_kp)

    def test_saving_each_step_interleaved(self):
        all_snapshots = {}
        for ProcClass in TEST_WAITING_PROCESSES:
            proc = ProcClass.new_instance()
            ws = ProcessWaitSaver(proc)
            proc.start()

            all_snapshots[ProcClass] = ws.snapshots

            self._check_process_against_snapshots(ProcClass, ws.snapshots)

    def test_logging(self):
        class LoggerTester(Process):
            def _run(self, **kwargs):
                self.logger.info("Test")

        # TODO: Test giving a custom logger to see if it gets used
        p = LoggerTester.new_instance()
        p.run()

    def test_abort(self):
        p = WaitForSignalProcess.new_instance()
        t = threading.Thread(target=p.start)

        # Start the process
        t.start()

        # Wait until it is running
        wait_until_state(p, ProcessState.RUNNING)
        self.assertEqual(p.state, ProcessState.RUNNING)

        # Abort it
        self.assertTrue(p.abort())

        # Wait until it's completely finished
        wait_until_destroyed(p)
        self.assertEqual(p.state, ProcessState.DESTROYED)
        self.assertTrue(p.aborted)

        t.join()

    def _check_process_against_snapshots(self, proc_class, snapshots):
        for i, info in zip(range(0, len(snapshots)), snapshots):
            loaded = proc_class.create_from(info[1])

            ps = ProcessWaitSaver(loaded)
            # Run the process
            loaded.start()

            # Now check going backwards until running that the saved states match
            j = 1
            while True:
                if j >= min(len(snapshots), len(ps.snapshots)) or \
                                snapshots[-j] is ProcessState.STARTED:
                    break

                self.assertEqual(snapshots[-j], ps.snapshots[-j])
                j += 1

    def _check_process_against_snapshot(self, snapshot, proc):
        self.assertEqual(snapshot.state, proc.state)

        new_bundle = Bundle()
        proc.save_instance_state(new_bundle)
        self.assertEqual(snapshot.bundle, new_bundle,
                         "Bundle mismatch with process class {}\n"
                         "Snapshot:\n{}\n"
                         "Loaded:\n{}".format(
                             proc.__class__, snapshot.bundle, new_bundle))

        self.assertEqual(snapshot.outputs, proc.outputs,
                         "Outputs mismatch with process class {}\n"
                         "Snapshot:\n{}\n"
                         "Loaded:\n{}".format(
                             proc.__class__, snapshot.outputs, proc.outputs))
