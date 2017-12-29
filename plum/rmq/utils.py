import os
import plum
import socket

# The key used in messages to give information about the host that send a message
HOST_KEY = 'host'
HOSTNAME_KEY = 'hostname'
PID_KEY = 'pid'
RESPONSE_KEY = 'response'
RESULT_KEY = 'result'
EXCEPTION_KEY = 'exception'
CANCELLED_KEY = 'cancelled'
PENDING_KEY = 'pending'


def get_host_info():
    return {
        'hostname': socket.gethostname(),
        'pid': os.getpid()
    }


def add_host_info(msg):
    if HOST_KEY in msg:
        raise ValueError("Host information key already exists in message")

    msg[HOST_KEY] = get_host_info()


def result_response(result):
    return {RESULT_KEY: result}


def exception_response(exception):
    return {EXCEPTION_KEY: exception}


def cancelled_response(msg=None):
    return {CANCELLED_KEY: msg}


def pending_response(msg=None):
    return {PENDING_KEY: msg}


# def result(response):
#     try:
#         raise plum.CancelledError(response[CANCELLED_KEY])
#     except KeyError:
#         pass
#     try:
#         msg = response[PENDING_KEY]
#         if msg is None:
#             msg = 'Result is not ready'
#         raise plum.InvalidStateError(msg)
#     except KeyError:
#         pass
#


def response_to_future(response, future=None):
    if future is None:
        future = plum.Future()

    if CANCELLED_KEY in response:
        future.cancel()
    elif EXCEPTION_KEY in response:
        future.set_exception(BaseException(response[EXCEPTION_KEY]))
    elif RESULT_KEY in response:
        future.set_result(response)

    return future
