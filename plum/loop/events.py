from plum.loop.object import LoopObject


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


class Timer(LoopObject):
    def __init__(self, when, callback, args):
        super(Timer, self).__init__()

        self._when = when
        self._callback = callback
        self._args = args
        self._cancelled = False

    def tick(self):
        if self._cancelled:
            self.loop().remove(self)
        elif self.loop().time() >= self._when:
            self.loop().call_soon(self._callback, *self._args)
            self.loop().remove(self)

    def cancel(self):
        if not self._cancelled:
            self._cancelled = True
            self._callback = None
            self._args = None

            if self.loop() is not None:
                self.loop().remove(self)
