import plum
from plum.communications import Action


class MessageAction(Action):
    def __init__(self, message):
        super(MessageAction, self).__init__()
        self._message = message

    def execute(self, publisher):
        plum.chain(publisher.action_message(self._message), self)
