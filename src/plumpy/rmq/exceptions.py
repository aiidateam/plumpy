# -*- coding: utf-8 -*-
import kiwipy
from aio_pika.exceptions import ChannelInvalidStateError, ConnectionClosed

__all__ = [
    'CommunicatorChannelInvalidStateError',
    'CommunicatorConnectionClosed',
]

# Alias aio_pika
CommunicatorConnectionClosed = ConnectionClosed
CommunicatorChannelInvalidStateError = ChannelInvalidStateError

CancelledError = kiwipy.CancelledError
