# -*- coding: utf-8 -*-


class EventHelper(object):
    def __init__(self, listener_type):
        assert(listener_type is not None)
        self._listener_type = listener_type
        self._listeners = []

    def add_listener(self, listener):
        assert(isinstance(listener, self._listener_type))
        self._listeners.append(listener)

    def remove_listener(self, listener):
        self._listeners.append(listener)

    @property
    def listeners(self):
        return self._listeners

    def fire_event(self, event_function, *args, **kwargs):
        # TODO: Check if the function is in the listener type
        for l in self.listeners:
            getattr(l, event_function)(*args, **kwargs)


class Sink(object):
    def __init__(self, type):
        self._type = type
        self._current_value = None

    def __str__(self):
        return "({}){}".format(self._type, self._current_value)

    def push(self, value):
        if value is None:
            raise ValueError("Cannot fill a sink with None")
        if self._type is not None and not isinstance(value, self._type):
            raise TypeError(
                "Sink expects values of type {}".format(self._type))

        self._current_value = value

    def pop(self):
        if not self.is_filled():
            raise RuntimeError("Sink has no value")

        val = self._current_value
        self._current_value = None
        return val

    def is_filled(self):
        return self._current_value is not None
