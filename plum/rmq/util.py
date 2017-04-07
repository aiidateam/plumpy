import os
import socket
import threading

from abc import ABCMeta, abstractmethod
from collections import Sequence

from plum.util import override


class SubscriberThread(threading.Thread):
    def __init__(self, create_connection, create_subscriber, poll_time=None, name=None):
        super(SubscriberThread, self).__init__(name=name)
        # Daemonise so that we quit if the main thread does
        self.daemon = True

        self._create_connection = create_connection
        self._create_subscriber = create_subscriber

        self._poll_time = poll_time
        self._started = threading.Event()
        self._stop = threading.Event()

    @override
    def run(self):
        connection = self._create_connection()
        subscribers = []
        try:
            subscribers = self._create_subscriber(connection)
            if isinstance(subscribers, Subscriber):
                subscribers = [subscribers]
            elif not isinstance(subscribers, Sequence):
                raise ValueError(
                    "{} did not return either a Subscriber or a list of "
                    "them".format(self._create_subscriber))

            args = {} if self._poll_time is None else {'time_limit': self._poll_time}
            self._started.set()
            while not self._stop.is_set():
                for subscriber in subscribers:
                    subscriber.poll(**args)
        finally:
            for sub in subscribers:
                sub.shutdown()
            connection.close()

    def set_poll_time(self, poll_time):
        self._poll_time = poll_time

    def stop(self):
        self._stop.set()

    def wait_till_started(self, timeout=None):
        return self._started.wait(timeout)


class Subscriber(object):
    """
    An abstract class that defines an interface that subscribers should conform
    to.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def poll(self, time_limit=None):
        """
        Poll for events

        :param time_limit: How long to poll for
        :type time_limit: float
        :return: The number of events consumed
        :rtype: int
        """
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def shutdown(self):
        pass


# The key used in messages to give information about the host that send a message
HOST_KEY = 'host'


def add_host_info(msg):
    if HOST_KEY in msg:
        raise ValueError("Host information key already exists in message")

    msg[HOST_KEY] = {
        'hostname': socket.gethostname(),
        'pid': os.getpid()
    }
