from collections import namedtuple
import threading
import re

from plum.util import ListenContext

_WilcardEntry = namedtuple("_WildcardEntry", ['re', 'listeners'])


class Mailman(object):
    """
    A class to send messages to listeners
    
    Messages send by this class:
    * mailman.listener_added.[subject]
    * mailman.listener_removed.[subject]
    
    where subject is the subject the listener is listening for
    """

    @staticmethod
    def contains_wildcard(event):
        """
        Does the event string contain a wildcard.

        :param event: The event string
        :type event: str or unicode
        :return: True if it does, False otherwise
        """
        return event.find('*') != -1 or event.find('#') != -1

    def __init__(self, loop):
        """

        :param loop: The event loop
        :type loop: :class:`plum.loop.event_loop.AbstractEventLoop`
        """
        self.__loop = loop
        self._specific_listeners = {}
        self._wildcard_listeners = {}
        self._listeners_lock = threading.Lock()

    def add_listener(self, listener, subject='*'):
        """
        Start listening to a particular event or a group of events.

        :param listener: The listener callback function to call when the
            event happens
        :param subject: A subject string
        :type subject: str or unicode
        """
        if subject is None:
            raise ValueError("Invalid event '{}'".format(subject))

        with self._listeners_lock:
            self._check_listener(listener)
            if self.contains_wildcard(subject):
                self._add_wildcard_listener(listener, subject)
            else:
                self._add_specific_listener(listener, subject)

    def remove_listener(self, listener, subject=None):
        """
        Stop listening for events.  If event is not specified it is assumed
        that the listener wants to stop listening to all events.

        :param listener: The listener that is currently listening
        :param subject: (optional) subject to stop listening for
        :type subject: str or unicode
        """
        with self._listeners_lock:
            if subject is None:
                # This means remove ALL messages for this listener
                for evt in self._specific_listeners.keys():
                    self._remove_specific_listener(listener, evt)
                for evt in self._wildcard_listeners.keys():
                    self._remove_wildcard_listener(listener, evt)
            else:
                if self.contains_wildcard(subject):
                    try:
                        self._remove_wildcard_listener(listener, subject)
                    except KeyError:
                        pass
                else:
                    try:
                        self._remove_specific_listener(listener, subject)
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

    def send(self, subject, body=None):
        """
        Send a message
        
        :param subject: The message subject 
        :param body: The body of the message
        """
        # These loops need to use copies because, e.g., the recipient may
        # add or remove listeners during the delivery

        # Deal with the wildcard listeners
        for evt, entry in self._wildcard_listeners.items():
            if self._wildcard_match(evt, subject):
                for l in list(entry.listeners):
                    self._deliver_msg(l, subject, body)

        # And now with the specific listeners
        try:
            for l in self._specific_listeners[subject].copy():
                self._deliver_msg(l, subject, body)
        except KeyError:
            pass

    def specific_listeners(self):
        return self._specific_listeners

    def wildcard_listeners(self):
        return self._wildcard_listeners

    def _deliver_msg(self, listener, event, body):
        self.__loop.call_soon(listener, self.__loop, event, body)

    @staticmethod
    def _check_listener(listener):
        if not callable(listener):
            raise ValueError("Listener must be callable")
            # Can do more sophisticated checks here, but it's a pain (to check both
            # classes that are callable having the right signature and plain functions)

    def _add_wildcard_listener(self, listener, subject):
        if subject in self._wildcard_listeners:
            self._wildcard_listeners[subject].listeners.add(listener)
        else:
            # Build the regular expression
            regex = subject.replace('.', '\.').replace('*', '.*').replace('#', '.+')
            self._wildcard_listeners[subject] = _WilcardEntry(re.compile(regex), {listener})

        self.send('mailman.listener_added.{}'.format(subject))

    def _remove_wildcard_listener(self, listener, subject):
        """
        Remove a wildcard listener.
        Precondition: listener in self._wildcard_listeners[event]

        :param listener: The listener to remove
        :param subject: The subject to stop listening for
        """
        self._wildcard_listeners[subject].listeners.discard(listener)
        if len(self._wildcard_listeners[subject].listeners) == 0:
            del self._wildcard_listeners[subject]

        self.send('mailman.listener_removed.{}'.format(subject))

    def _add_specific_listener(self, listener, subject):
        self._specific_listeners.setdefault(subject, set()).add(listener)

        self.send('mailman.listener_added.{}'.format(subject))

    def _remove_specific_listener(self, listener, subject):
        """
        Remove a specific listener.
        Precondition: listener in self._specific_listeners[event]

        :param listener: The listener to remove
        :param subject: The subject to stop listening for
        """
        self._specific_listeners[subject].discard(listener)
        if len(self._specific_listeners[subject]) == 0:
            del self._specific_listeners[subject]

        self.send('mailman.listener_removed.{}'.format(subject))

    def _wildcard_match(self, event, to_match):
        return self._wildcard_listeners[event].re.match(to_match) is not None
