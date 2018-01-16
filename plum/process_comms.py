import communications

__all__ = ['ProcessReceiver', 'PAUSE_MSG', 'PLAY_MSG', 'CANCEL_MSG', 'STATUS_MSG']

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
