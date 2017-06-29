


class DirectExecutor(object):
    class Future(object):
        def __init__(self):
            self._result = None
            self._exception = None

        def result(self, timeout=None):
            if self._exception is None:
                return self._result
            else:
                raise self._exception

        def exception(self, timeout=None):
            return self._exception

        def set_result(self, result):
            if self._exception is not None:
                raise RuntimeError("Exception already set")

            self._result = result

        def set_exception(self, exception):
            if self._result is not None:
                raise RuntimeError("Result already set")

            self._exception = exception

    @staticmethod
    def submit(fn, *args, **kwargs):
        future = DirectExecutor.Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except BaseException as e:
            future.set_exception(e)

        return future
