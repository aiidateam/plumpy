import os
import socket
import threading

from abc import ABCMeta, abstractmethod
from collections import Sequence

from plum.util import override

# The key used in messages to give information about the host that send a message
HOST_KEY = 'host'


def add_host_info(msg):
    if HOST_KEY in msg:
        raise ValueError("Host information key already exists in message")

    msg[HOST_KEY] = {
        'hostname': socket.gethostname(),
        'pid': os.getpid()
    }
