"""
Keep track of the per-thread call stack of processes.
"""

import contextlib
import threading

# Use thread-local storage for the stack
_thread_local = threading.local()


@contextlib.contextmanager
def in_stack(process):
    push(process)
    try:
        yield
    finally:
        pop(process)


def top():
    return _stack()[-1]


def stack():
    """Get an immutable tuple of the stack"""
    return tuple(_stack())


def push(process):
    _stack().append(process)


def is_empty():
    return len(_stack()) == 0


def pop(process):
    """
    Pop a process from the stack.  To make sure the stack is not corrupted
    the process instance of the calling process should be supplied
    so we can verify that is really is top of the stack.

    :param process: The process instance
    """
    global _thread_local
    assert process is top(), "Can't pop a process that is not top of the stack"
    _stack().pop()


def _stack():
    """Access the private live stack"""
    global _thread_local
    try:
        return _thread_local.wf_stack
    except AttributeError:
        _thread_local.wf_stack = []
        return _thread_local.wf_stack
