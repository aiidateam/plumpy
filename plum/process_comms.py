from . import communications
from . import futures

__all__ = ['ProcessReceiver', 'PAUSE_MSG', 'PLAY_MSG', 'CANCEL_MSG', 'STATUS_MSG',
           'ProcessAction', 'PlayAction', 'PauseAction', 'CancelAction', 'StatusAction']

INTENT_KEY = 'intent'


class Intent(object):
    PLAY = 'play'
    PAUSE = 'pause'
    CANCEL = 'cancel'
    STATUS = 'status'


PAUSE_MSG = {INTENT_KEY: Intent.PAUSE}
PLAY_MSG = {INTENT_KEY: Intent.PLAY}
CANCEL_MSG = {INTENT_KEY: Intent.CANCEL}
STATUS_MSG = {INTENT_KEY: Intent.STATUS}


class ProcessReceiver(communications.Receiver):
    """
    Responsible for receiving messages and translating them to actions
    on the process.
    """

    def __init__(self, process):
        """
        :param process: :class:`plum.Process`
        """
        self._process = process

    def on_rpc_receive(self, msg):
        intent = msg['intent']
        if intent == Intent.PLAY:
            return self._process.play()
        elif intent == Intent.PAUSE:
            return self._process.pause()
        elif intent == Intent.CANCEL:
            return self._process.cancel(msg=msg.get('msg', None))
        elif intent == Intent.STATUS:
            status_info = {}
            self._process.get_status_info(status_info)
            return status_info
        else:
            raise RuntimeError("Unknown intent")

    def on_broadcast_receive(self, msg):
        pass


class ProcessAction(communications.Action):
    """ Generic process action """

    def __init__(self, pid, msg):
        """
        :param pid: The process ID
        :param msg: The action message to deliver to the process
        """
        super(ProcessAction, self).__init__()
        self._pid = pid
        self._msg = msg

    def execute(self, publisher):
        future = publisher.rpc_send(self._pid, self._msg)
        futures.chain(future, self)


class PauseAction(ProcessAction):
    def __init__(self, pid):
        super(PauseAction, self).__init__(pid, PAUSE_MSG)


class PlayAction(ProcessAction):
    def __init__(self, pid):
        super(PlayAction, self).__init__(pid, PLAY_MSG)


class StatusAction(ProcessAction):
    def __init__(self, pid):
        super(StatusAction, self).__init__(pid, STATUS_MSG)


class CancelAction(ProcessAction):
    def __init__(self, pid):
        super(CancelAction, self).__init__(pid, CANCEL_MSG)
