import functools
import plumpy
import unittest

from plumpy import ProcessState


class TestCase(unittest.TestCase):
    pass


class TestCaseWithLoop(TestCase):
    def setUp(self):
        super(TestCaseWithLoop, self).setUp()
        self.loop = plumpy.new_event_loop()
        plumpy.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()
        self.loop = None
        plumpy.set_event_loop(None)


def get_message(receive_list, loop, subject, to, body, sender_id):
    receive_list.append({
        'subject': subject,
        'to': to,
        'body': body,
        'sender_id': sender_id
    })


def get_message_capture_fn(capture_list):
    return functools.partial(get_message, capture_list)


def run_loop_with_timeout(loop, timeout=2.):
    loop.call_later(timeout, loop.stop)
    loop.start()


def run_until_waiting(proc):
    """ Set up a future that will be resolved on entering the WAITING state """
    listener = plumpy.ProcessListener()
    in_waiting = plumpy.Future()

    if proc.state == ProcessState.WAITING:
        in_waiting.set_result(True)
    else:
        def on_waiting(waiting_proc):
            in_waiting.set_result(True)
            proc.remove_process_listener(listener)

        listener.on_process_waiting = on_waiting
        proc.add_process_listener(listener)

    return in_waiting


def run_until_paused(proc):
    """ Set up a future that will be resolved on entering the WAITING state """
    listener = plumpy.ProcessListener()
    paused = plumpy.Future()

    if proc.paused:
        paused.set_result(True)
    else:
        def on_paused(_paused_proc):
            paused.set_result(True)
            proc.remove_process_listener(listener)

        listener.on_process_paused = on_paused
        proc.add_process_listener(listener)

    return paused
