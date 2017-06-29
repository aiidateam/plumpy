from abc import ABCMeta, abstractmethod
from collections import namedtuple
import logging
import re
import threading
import traceback

from plum import Process
from plum.loop import LoopListener
from plum.process_listener import ProcessListener
from plum.util import override, protected, ListenContext
from plum.wait import WaitOn, Unsavable, WaitEvent
from plum.exceptions import Interrupted

_LOGGER = logging.getLogger(__name__)

_WilcardEntry = namedtuple("_WildcardEntry", ['re', 'listeners'])


class ProcessEventEmitter(LoopListener, ProcessListener):
    def __init__(self, loop=None):
        self.__loop = None
        if loop is not None:
            self.start_emitting(loop)

    def start_emitting(self, loop):
        self.__loop = loop
        self.__loop.add_loop_listener(self)

    def stop_emitting(self):
        self.__loop.remove_loop_listener(self)
        self.__loop = None

    # region LoopListener
    def on_object_inserted(self, loop, loop_object):
        if isinstance(loop_object, Process):
            loop_object.add_process_listener(self)

    def on_object_removed(self, loop, loop_object):
        if isinstance(loop_object, Process):
            loop_object.remove_process_listener(self)

    # endregion LoopListener

    # region ProcessListener
    def on_process_start(self, process):
        self._emit_message(process, 'start')

    def on_process_run(self, process):
        self._emit_message(process, 'run')

    def on_process_wait(self, process):
        self._emit_message(process, 'wait')

    def on_process_resume(self, process):
        self._emit_message(process, 'resume')

    def on_process_abort(self, process):
        self._emit_message(process, 'abort')

    def on_output_emitted(self, process, output_port, value, dynamic):
        self._emit_message(process, 'emitted')

    def on_process_finish(self, process):
        self._emit_message(process, 'finish')

    def on_process_stop(self, process):
        self._emit_message(process, 'stop')

    def on_process_fail(self, process):
        self._emit_message(process, 'fail')

    def on_process_terminate(self, process):
        self._emit_message(process, 'terminate')

    # endregion ProcessListener

    def _emit_message(self, process, message, body=None):
        self.__loop.messages().send('process.{}.{}'.format(process.pid, message), body)


class EventEmitter(object):
    """
    A class to send general events to listeners
    """
    __metaclass__ = ABCMeta

    @staticmethod
    def contains_wildcard(eventstring):
        """
        Does the event string contain a wildcard.

        :param eventstring: The event string
        :type eventstring: str or unicode
        :return: True if it does, False otherwise
        """
        return eventstring.find('*') != -1 or eventstring.find('#') != -1

    def __init__(self):
        self._specific_listeners = {}
        self._wildcard_listeners = {}
        self._listeners_lock = threading.Lock()

    def start_listening(self, listener, event='*'):
        """
        Start listening to a particular event or a group of events.

        :param listener: The listener callback function to call when the
            event happens
        :param event: An event string
        :type event: str or unicode
        """
        with self._listeners_lock:
            self._check_listener(listener)
            if self.contains_wildcard(event):
                self._add_wildcard_listener(listener, event)
            else:
                self._add_specific_listener(listener, event)

    def stop_listening(self, listener, event=None):
        """
        Stop listening for events.  If event is not specified it is assumed
        that the listener wants to stop listening to all events.

        :param listener: The listener that is currently listening
        :param event: (optional) event to stop listening for
        :type event: str or unicode
        """
        with self._listeners_lock:
            if event is None:
                # This means remove ALL messages for this listener
                for evt in self._specific_listeners.iterkeys():
                    self._remove_specific_listener(listener, evt)
                for evt in self._wildcard_listeners.iterkeys():
                    self._remove_wildcard_listener(listener, evt)
            else:
                if self.contains_wildcard(event):
                    try:
                        self._remove_wildcard_listener(listener, event)
                    except KeyError:
                        pass
                else:
                    try:
                        self._remove_specific_listener(listener, event)
                    except KeyError:
                        pass

    def listen_scope(self, listener, event=None):
        return ListenContext(self, listener, event)

    def clear_all_listeners(self):
        with self._listeners_lock:
            self._specific_listeners.clear()
            self._wildcard_listeners.clear()

    def num_listening(self):
        """
        Get the number of events that are being listening for.  This
        corresponds exactly to the number of .start_listening() calls made
        this this emitter.

        :return: The number of events listened for
        :rtype: int
        """
        with self._listeners_lock:
            total = 0
            for listeners in self._specific_listeners.itervalues():
                total += len(listeners)
            for entry in self._wildcard_listeners.itervalues():
                total += len(entry.listeners)
            return total

    def event_occurred(self, event, body=None):
        # These loops need to use copies because, e.g., the recipient may
        # add or remove listeners during the delivery

        # Deal with the wildcard listeners
        for evt, entry in self._wildcard_listeners.items():
            if self._wildcard_match(evt, event):
                for l in list(entry.listeners):
                    self.deliver_msg(l, event, body)

        # And now with the specific listeners
        try:
            for l in self._specific_listeners[event].copy():
                self.deliver_msg(l, event, body)
        except KeyError:
            pass

    @protected
    def deliver_msg(self, listener, event, body):
        try:
            listener(self, event, body)
        except BaseException:
            _LOGGER.error("Exception deliverying message to {}:\n{}".format(
                listener, traceback.format_exc()))

    @protected
    def get_specific_listeners(self):
        return self._specific_listeners

    @protected
    def get_wildcard_listeners(self):
        return self._wildcard_listeners

    @staticmethod
    def _check_listener(listener):
        if not callable(listener):
            raise ValueError("Listener must be callable")
            # Can do more sophisticated checks here, but it's a pain (to check both
            # classes that are callable having the right signature and plain functions)

    def _add_wildcard_listener(self, listener, event):
        if event in self._wildcard_listeners:
            self._wildcard_listeners[event].listeners.add(listener)
        else:
            # Build the regular expression
            regex = event.replace('.', '\.').replace('*', '.*').replace('#', '.+')
            self._wildcard_listeners[event] = _WilcardEntry(re.compile(regex), {listener})

    def _remove_wildcard_listener(self, listener, event):
        """
        Remove a wildcard listener.
        Precondition: listener in self._wildcard_listeners[event]

        :param listener: The listener to remove
        :param event: The event to stop listening for
        """
        self._wildcard_listeners[event].listeners.discard(listener)
        if len(self._wildcard_listeners[event].listeners) == 0:
            del self._wildcard_listeners[event]

    def _add_specific_listener(self, listener, event):
        self._specific_listeners.setdefault(event, set()).add(listener)

    def _remove_specific_listener(self, listener, event):
        """
        Remove a specific listener.
        Precondition: listener in self._specific_listeners[event]

        :param listener: The listener to remove
        :param event: The event to stop listening for
        """
        self._specific_listeners[event].discard(listener)
        if len(self._specific_listeners[event]) == 0:
            del self._specific_listeners[event]

    def _wildcard_match(self, event, to_match):
        return self._wildcard_listeners[event].re.match(to_match) is not None


class WaitOnEvent(WaitOn):
    def __init__(self, event):
        """
        :param event: The event to listen for
        :type event: str or unicode
        """
        super(WaitOnEvent, self).__init__()
        self._event = event
        self._body = None
        self._future = None

    def __str__(self):
        return "waiting on: {}".format(self._event)

    @override
    def get_future(self, loop):
        if self._future is None:
            loop.messages().start_listening(self._event_occurred, self._event)
            self._future = loop.create_future()

        return self._future

    @override
    def save_instance_state(self, out_state):
        super(WaitOnEvent, self).save_instance_state(out_state)
        out_state['event'] = self._event
        out_state['body'] = self._body

    @override
    def load_instance_state(self, saved_state):
        super(WaitOnEvent, self).load_instance_state(saved_state)
        self._event = saved_state['event']
        self._body = saved_state['body']

    def get_event(self):
        return self._event

    def get_body(self):
        return self._body

    def _event_occurred(self, loop, event, body):
        self._event = event
        self._body = body
        self._future.set_result((self._event, self._body))
        loop.messages().stop_listening(self._event_occurred)


def wait_on_process_event(pid='*', event='*'):
    return WaitOnEvent("process.{}.{}".format(pid, event))