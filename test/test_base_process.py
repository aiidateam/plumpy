import unittest
import plumpy
from plumpy import base_process
from plumpy.base_process import ProcessStateMachine, ProcessState, Wait, Continue


def execute(proc):
    """ Execute a process state machine """
    while proc.state in [ProcessState.CREATED, ProcessState.RUNNING]:
        if proc.state in [ProcessState.CREATED]:
            proc.start()
        elif proc.state == ProcessState.RUNNING:
            proc._state._run()

    if proc.done():
        return proc.result()


class SimpleProc(ProcessStateMachine):
    def do_run(self):
        while self.state in [ProcessState.CREATED, ProcessState.RUNNING]:
            if self.state in [ProcessState.CREATED]:
                self.start()
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

    def test_state_saving_created(self):
        class FauxProc(object):
            def dummy_fn(self):
                pass

        proc = FauxProc()
        created1 = base_process.Created(proc, proc.dummy_fn, 'hello', some_kw='goodbye')
        saved_state = created1.save()
        created2 = plumpy.Savable.load(saved_state, plumpy.LoadContext(process=proc))
        saved_state2 = created2.save()
        self.assertDictEqual(saved_state, saved_state2)
        self._attributes_match(created1, created2)

    def test_except(self):
        class ExceptingProc(SimpleProc):
            def run(self):
                raise RuntimeError("You're on yer own pal")

        p = ExceptingProc()
        with self.assertRaises(RuntimeError):
            p.do_run()
        self.assertIsNotNone(p.exception())

    def test_immediate_kill(self):
        """ Check that if a process is killed from within it's method
         then it is actioned immediately"""

        class Proc(ProcessStateMachine):
            after_kill = False

            def run(self):
                self.kill("Kill immediately")
                self.after_kill = True

        proc = Proc()
        with self.assertRaises(base_process.KilledError):
            execute(proc)
        self.assertFalse(proc.after_kill)
        self.assertEqual(proc.state, ProcessState.KILLED)

    def _attributes_match(self, a, b):
        self.assertDictEqual(a.__dict__, b.__dict__)


def dummy_fn():
    pass


class TestCommandPersistence(unittest.TestCase):
    def test_continue(self):
        fut = plumpy.SavableFuture()
        contninue_cmd = plumpy.Continue(dummy_fn, 'arg', kwarg='kwarg')
        saved_state = contninue_cmd.save()
        continue_cmd_loaded = plumpy.Continue.recreate_from(saved_state)

        for attr in ('args', 'kwargs', 'continue_fn'):
            self.assertEquals(getattr(contninue_cmd, attr), getattr(continue_cmd_loaded, attr))

    def test_stop(self):
        stop_cmd = plumpy.Stop("I'm done")
        saved_state = stop_cmd.save()
        stop_cmd_loaded = plumpy.Stop.recreate_from(saved_state)
        self.assertEquals(stop_cmd.result, stop_cmd_loaded.result)

    def test_wait(self):
        fut = plumpy.SavableFuture()
        wait_cmd = plumpy.Wait(fut, dummy_fn, msg='Waiting for nothing')
        saved_state = wait_cmd.save()
        wait_cmd_loaded = plumpy.Wait.recreate_from(saved_state)

        for attr in ('continue_fn', 'msg'):
            self.assertEquals(getattr(wait_cmd, attr), getattr(wait_cmd_loaded, attr))
