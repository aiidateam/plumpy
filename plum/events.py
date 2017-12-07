import trollius

__all__ = ['new_event_loop', 'set_event_loop', 'get_event_loop']

get_event_loop = trollius.get_event_loop
new_event_loop = trollius.new_event_loop

#
# class ProcessEventLoop(trollius.BaseSelectorEventLoop):
#     def __init__(self):
#         super(ProcessEventLoop, self).__init__()
#         self._processes = {}
#
#     def _insert_process(self, process):
#         if process.pid in self._processes:
#             raise RuntimeError("Cannot insert the same process twice")
#
#         self._processes[process.pid] = process
#         process.add_done_callback(self._process_done)
#
#     def _process_done(self, process):
#         del self._processes[process.pid]
#
#     def get_process(self, pid):
#         try:
#             return self._processes[pid]
#         except KeyError:
#             raise ValueError("Process with pid '{}' not known".format(pid))
#
#     def run_until_complete(self, awaitable):
#         # Some awaitables are 'playable', these should be played now,
#         # otherwise they won't do anything and will never complete
#         try:
#             awaitable.play()
#         except AttributeError:
#             pass
#         return super(ProcessEventLoop, self).run_until_complete(awaitable)
#
#     def create(self, object_type, *args, **kwargs):
#         kwargs['loop'] = self
#         return object_type(*args, **kwargs)


def set_event_loop(event_loop):
    # assert isinstance(event_loop, ProcessEventLoop), "Must be a ProcessEventLoop"
    trollius.set_event_loop(event_loop)

# _policy = apricotpy.events.BaseDefaultEventLoopPolicy()
# _policy._loop_factory = ProcessEventLoop
# apricotpy.set_event_loop_policy(_policy)
