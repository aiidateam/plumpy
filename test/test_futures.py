import plumpy
import tornado.gen
import tornado.concurrent

from . import utils


def func(arg):
    return arg


def except_func():
    raise RuntimeError


@tornado.gen.coroutine
def coro(arg):
    fut = tornado.concurrent.Future()
    fut.set_result(True)
    yield fut
    yield fut
    raise tornado.gen.Return(arg)


class TestTask(utils.TestCaseWithLoop):
    def test_fn_task(self):
        RETVAL = 'ret this!'
        result = plumpy.run_until_complete(plumpy.Task(func, RETVAL))
        self.assertEquals(RETVAL, result)

    def test_exception_fn_task(self):
        with self.assertRaises(RuntimeError):
            plumpy.run_until_complete(plumpy.Task(except_func()))

    def test_coro_task(self):
        RETVAL = 'coro this!'
        result = plumpy.run_until_complete(plumpy.Task(coro, RETVAL))
        self.assertEquals(RETVAL, result)

