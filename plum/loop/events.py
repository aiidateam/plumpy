from . import objects
from . import tasks

class Handle(object):
    def __init__(self, fn, args, loop):
        self._loop = loop
        self._fn = fn
        self._args = args
        self._cancelled = False

    def cancel(self):
        if not self._cancelled:
            self._cancelled = True
            self._fn = None
            self._args = None

    def _run(self):
        self._fn(*self._args)


class Timer(objects.Ticking, objects.Awaitable, objects.LoopObject):
    def __init__(self, loop, when, callback, args):
        super(Timer, self).__init__(loop)

        self._when = when
        self._callback = callback
        self._args = args

    def tick(self):
        time = self.loop().time()
        if time >= self._when:
            self.pause()
            self.loop().call_soon(self._callback, *self._args)
            self.set_result(time)


