import trollius

__all__ = ['new_event_loop', 'set_event_loop', 'get_event_loop']

get_event_loop = trollius.get_event_loop
new_event_loop = trollius.new_event_loop
set_event_loop = trollius.set_event_loop
