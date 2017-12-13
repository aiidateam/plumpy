from tornado import ioloop

__all__ = ['new_event_loop', 'set_event_loop', 'get_event_loop']

get_event_loop = ioloop.IOLoop.current
new_event_loop = ioloop.IOLoop


def set_event_loop(loop):
    loop.make_current()
