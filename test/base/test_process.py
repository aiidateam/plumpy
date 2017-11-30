import unittest
from plum.base import ProcessStateMachine, ProcessState, Wait, Continue


class TestProcess(unittest.TestCase):
    def test_basic(self):
        class MyProc(ProcessStateMachine):
            def run(self):
                return True

        p = MyProc()
        p.play()
        self.assertTrue(p.result())

    def test_continue(self):
        class MyProc(ProcessStateMachine):
            def run(self):
                return Continue(self.step2)

            def step2(self):
                return True

        p = MyProc()
        p.play()
        self.assertTrue(p.result())

    def test_waiting(self):
        class MyProc(ProcessStateMachine):
            def run(self):
                return Wait(self.step2, msg='Waiting for the end of time')

            def step2(self):
                return True

        p = MyProc()
        p.play()
        self.assertEqual(p.state, ProcessState.WAITING)
        d = {}
        p.save_instance_state(d)
        print(p)
        p.resume()
        print(p)

    def test_fail(self):
        class FailProc(ProcessStateMachine):
            def run(self):
                raise RuntimeError("You're on yer own pal")

        p = FailProc()
        p.play()
        self.assertIsNotNone(p.exception())
        with self.assertRaises(RuntimeError):
            p.result()
