import unittest
from plum.base import ProcessStateMachine, ProcessState, Wait, Continue


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

        result = MyProc().do_run()
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
        print(p)
        p.resume()
        print(p)

    # def test_fail(self):
    #     class FailProc(TestProc):
    #         def run(self):
    #             raise RuntimeError("You're on yer own pal")
    #
    #     p = FailProc()
    #     p.do_run()
    #     self.assertIsNotNone(p.exception())
    #     with self.assertRaises(RuntimeError):
    #         p.result()
