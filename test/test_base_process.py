import unittest
import plumpy
from plumpy import base_process
from plumpy.base_process import ProcessStateMachine, ProcessState, Wait, Continue


def execute(proc):
    """ Execute a process state machine """
    while proc.state in [ProcessState.CREATED, ProcessState.PAUSED, ProcessState.RUNNING]:
        if proc.state in [ProcessState.CREATED, ProcessState.PAUSED]:
            proc.play()
        elif proc.state == ProcessState.RUNNING:
            proc._state._run()

    if proc.done():
        return proc.result()


class SimpleProc(ProcessStateMachine):
    def do_run(self):
        while self.state in [ProcessState.CREATED, ProcessState.PAUSED, ProcessState.RUNNING]:
            if self.state in [ProcessState.CREATED, ProcessState.PAUSED]:
                self.play()
            elif self.state == ProcessState.RUNNING:
                self._state._run()

        if self.done():
            return self.result()


class TestProcess(unittest.TestCase):
    def test_basic(self):
        class MyProc(SimpleProc):
            def run(self):
                return True

        result = execute(MyProc())
        self.assertTrue(result)

    def test_continue(self):
        class MyProc(SimpleProc):
            def run(self):
                return Continue(self.step2)

            def step2(self):
                return True

        proc = MyProc()
        result = proc.do_run()
        self.assertTrue(result)

    def test_waiting(self):
        class MyProc(SimpleProc):
            def run(self):
                return Wait(self.step2, msg='Waiting for the end of time')

            def step2(self):
                return True

        p = MyProc()
        p.do_run()
        self.assertEqual(p.state, ProcessState.WAITING)
        p.resume()

    def test_state_saving_created(self):
        class FauxProc(object):
            def dummy_fn(self):
                pass

        proc = FauxProc()
        created1 = base_process.Created(proc, proc.dummy_fn, 'hello', some_kw='goodbye')
        saved_state = created1.save()
        created2 = plumpy.Savable.load(saved_state, proc)
        saved_state2 = created2.save()
        self.assertDictEqual(saved_state, saved_state2)
        self._attributes_match(created1, created2)

    def test_fail(self):
        class FailProc(SimpleProc):
            def run(self):
                raise RuntimeError("You're on yer own pal")

        p = FailProc()
        with self.assertRaises(RuntimeError):
            p.do_run()
        self.assertIsNotNone(p.exception())

    def test_immediate_cancel(self):
        """ Check that if a process is cancelled from within it's method
         then it is actioned immediately"""

        class Proc(ProcessStateMachine):
            after_cancel = False

            def run(self):
                self.cancel("Cancel immediately")
                self.after_cancel = True

        proc = Proc()
        with self.assertRaises(base_process.CancelledError):
            execute(proc)
        self.assertFalse(proc.after_cancel)
        self.assertEqual(proc.state, ProcessState.CANCELLED)

    def _attributes_match(self, a, b):
        self.assertDictEqual(a.__dict__, b.__dict__)
