# -*- coding: utf-8 -*-

import threading


class EventHelper(object):
    def __init__(self, listener_type):
        assert(listener_type is not None)
        self._listener_type = listener_type
        self._listeners = []

    def add_listener(self, listener):
        assert(isinstance(listener, self._listener_type))
        self._listeners.append(listener)

    def remove_listener(self, listener):
        self._listeners.remove(listener)

    @property
    def listeners(self):
        return self._listeners

    def fire_event(self, event_function, *args, **kwargs):
        # TODO: Check if the function is in the listener type
        # We have to use a copy here because the listener may
        # remove themselves during the message
        for l in list(self.listeners):
            getattr(l, event_function)(*args, **kwargs)


class ThreadSafeCounter(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.counter = 0

    def increment(self):
        with self.lock:
            self.counter += 1

    def decrement(self):
        with self.lock:
            self.counter -= 1

    @property
    def value(self):
        with self.lock:
            return self.counter
