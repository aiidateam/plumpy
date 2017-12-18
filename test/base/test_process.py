import unittest
import plum
from plum import base
from plum.base import process
from plum.base import ProcessStateMachine, ProcessState, Wait, Continue


def execute(proc):
    """ Execute a process state machine """
    while proc.state in [ProcessState.CREATED, ProcessState.PAUSED, ProcessState.RUNNING]:
        if proc.state in [ProcessState.CREATED, ProcessState.PAUSED]:
            proc.play()
        elif proc.state == ProcessState.RUNNING:
            proc._state._run()

    if proc.done():
        return proc.result()


class TestProc(ProcessStateMachine):
    def do_run(self):
        while self.state in [ProcessState.CREATED, ProcessState.PAUSED, ProcessState.RUNNING]:
            if self.state in [ProcessState.CREATED,
                              ProcessState.PAUSED]:
                self.play()
            elif self.state == ProcessState.RUNNING:
                self._state._run()

        if self.done():
            return self.result()


class TestProcess(unittest.TestCase):
    def test_basic(self):
        class MyProc(TestProc):
            def run(self):
                return True

        result = execute(MyProc())
        self.assertTrue(result)

    def test_continue(self):
        class MyProc(TestProc):
            def run(self):
                return Continue(self.step2)

            def step2(self):
                return True

        proc = MyProc()
        result = proc.do_run()
        self.assertTrue(result)

    def test_waiting(self):
        class MyProc(TestProc):
            def run(self):
                return Wait(self.step2, msg='Waiting for the end of time')

            def step2(self):
                return True

        p = MyProc()
        p.do_run()
        self.assertEqual(p.state, ProcessState.WAITING)
        d = {}
        p.save_state(d)
        p.resume()

    @unittest.skip("Until we have a way to know which attributes should be persisted, skip")
    def test_state_saving_created(self):
        created1 = process.Created(None, _dummy_fn, 'hello', some_kw='goodbye')
        saved_state = {}
        created1.save_instance_state(saved_state)
        created2 = load_state(process.Created, None, saved_state)
        self.assertTrue(self._attributes_match(created1, created2))

    def test_fail(self):
        class FailProc(TestProc):
            def run(self):
                raise RuntimeError("You're on yer own pal")

        p = FailProc()
        p.do_run()
        self.assertIsNotNone(p.exception())
        with self.assertRaises(RuntimeError):
            p.result()

    def test_immediate_cancel(self):
        """ Check that if a process is cancelled from within it's method
         then it is actioned immediately"""

        class Proc(ProcessStateMachine):
            after_cancel = False

            def run(self):
                self.cancel("Cancel immediately")
                self.after_cancel = True

        proc = Proc()
        with self.assertRaises(process.CancelledError):
            execute(proc)
        self.assertFalse(proc.after_cancel)
        self.assertEqual(proc.state, ProcessState.CANCELLED)

    def _attributes_match(self, a, b):
        self.assertDictEqual(a.__dict__, b.__dict__)


def load_state(state_class, process, saved_state):
    state = state_class.__new__(state_class)
    base.call_with_super_check(state.load_instance_state, process, saved_state)

    return state


def _dummy_fn():
    pass
