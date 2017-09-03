import apricotpy
import logging
import json

from plum.process_listener import ProcessListener
from plum.process_monitor import ProcessMonitorListener, MONITOR
from plum.rmq.defaults import Defaults
from plum.utils import fullname, override

PROC_INFO_KEY = 'proc_info'
DETAILS_KEY = 'details'

_LOGGER = logging.getLogger(__name__)


def _get_logger():
    global _LOGGER
    return _LOGGER


class ProcessEventPublisher(ProcessListener, ProcessMonitorListener):
    """
    This class publishes status updates from processes based on receiving event
    messages.
    """

    def __init__(self, connection, exchange=Defaults.EVENT_EXCHANGE, encoder=json.dumps):
        self._exchange = exchange
        self._encode = encoder
        self._processes = []

        # Set up communication
        self._channel = connection.channel()
        self._channel.exchange_declare(exchange=self._exchange, type='topic')

    def add_process(self, process):
        """
        Add a process to have its events be published

        :param process: The process to publish updates for
        :type process: :class:`plum.process.Process`
        """
        self._processes.append(process)
        process.add_process_listener(self)

    def remove_process(self, process):
        """
        Remove a process from having its events be published

        :param process: The process to stop publishing updates for
        :type process: :class:`plum.process.Process`
        """
        process.remove_process_listener(self)
        self._processes.remove(process)

    def reset(self):
        """
        Stop listening to all processes.
        """
        for p in self._processes:
            p.remove_process_listener(self)
        self._processes = []

    def enable_publish_all(self):
        """
        Publish event messages from all run processes.
        """
        MONITOR.add_listener(self)

    def disable_publish_all(self):
        """
        Stop publishing messages from all run processes.
        """
        MONITOR.remove_listener(self)

    def on_monitored_process_registered(self, process):
        self.add_process(process)

    # region From ProcessListener
    def on_process_start(self, process):
        key = "{}.start".format(process.pid)
        self._send_event_msg(process, key)

    def on_process_run(self, process):
        key = "{}.run".format(process.pid)
        self._send_event_msg(process, key)

    def on_process_wait(self, process):
        key = "{}.wait".format(process.pid)
        evt_details = {'waiting_on': str(process.get_waiting_on())}
        self._send_event_msg(process, key, {DETAILS_KEY: evt_details})

    def on_process_resume(self, process):
        key = "{}.resume".format(process.pid)
        self._send_event_msg(process, key)

    def on_process_finish(self, process):
        key = "{}.finish".format(process.pid)
        self._send_event_msg(process, key)

    def on_process_abort(self, process):
        key = "{}.abort".format(process.pid)
        self._send_event_msg(process, key)
        self.remove_process(process)

    def on_process_stop(self, process):
        key = "{}.stop".format(process.pid)
        self._send_event_msg(process, key)
        self.remove_process(process)

    def on_process_fail(self, process):
        key = "{}.fail".format(process.pid)
        exception = process.exception()
        evt_details = {'exception_type': fullname(exception), 'exception_msg': exception.message}
        self._send_event_msg(process, key, {DETAILS_KEY: evt_details})
        self.remove_process(process)

    def on_output_emitted(self, process, output_port, value, dynamic):
        key = "{}.emitted".format(process.pid)
        # Don't send the value, it could be large and/or unserialisable
        evt_details = {'port': output_port, 'dynamic': dynamic}
        self._send_event_msg(process, key, {DETAILS_KEY: evt_details})

    # endregion

    def _send_event_msg(self, process, key, msg=None):
        if msg is None:
            msg = {}
        self._add_process_info(msg, process)
        self._channel.basic_publish(self._exchange, key, body=self._encode(msg))

    def _add_process_info(self, msg, process):
        msg[PROC_INFO_KEY] = {'type': fullname(process)}


class ProcessEventSubscriber(apricotpy.TickingLoopObject):
    def __init__(self, connection, exchange=Defaults.EVENT_EXCHANGE, decoder=json.loads):
        super(ProcessEventSubscriber, self).__init__()

        self._exchange = exchange
        self._decode = decoder
        self._callbacks = set()

        # Set up connection
        self._channel = connection.channel()
        self._channel.exchange_declare(exchange=self._exchange, type='topic')
        result = self._channel.queue_declare(exclusive=True)
        self._queue = result.method.queue
        self._channel.queue_bind(exchange=self._exchange, queue=self._queue, routing_key='#')
        self._channel.basic_consume(self._on_event, queue=self._queue, no_ack=True)

    def add_event_callback(self, fn):
        self._callbacks.add(fn)

    def remove_event_callback(self, fn):
        self._callbacks.remove(fn)

    @override
    def tick(self):
        self._channel.connection.process_data_events()

    def _on_event(self, ch, method, props, body):
        for fn in self._callbacks:
            try:
                fn(method.routing_key, self._decode(body))
            except BaseException as e:
                _get_logger().warning(
                    "Event callback '{}' produced exception:\n{}".format(fn, e.message)
                )


def split_event(evt):
    return evt.split('.')
