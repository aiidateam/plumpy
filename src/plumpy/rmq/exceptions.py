# -*- coding: utf-8 -*-
from aio_pika.exceptions import ChannelInvalidStateError, ConnectionClosed

__all__ = [
    'CommunicatorChannelInvalidStateError',
    'CommunicatorConnectionClosed',
]

# Alias aio_pika
CommunicatorConnectionClosed = ConnectionClosed
CommunicatorChannelInvalidStateError = ChannelInvalidStateError
