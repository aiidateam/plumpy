from . import process


# region Commands
class Command(object):
    pass


class Cancel(Command):
    def __init__(self, msg=None):
        self.msg = msg


class Wait(Command):
    def __init__(self, continue_fn=None, desc=None):
        self.continue_fn = continue_fn
        self.desc = desc


class Stop(Command):
    def __init__(self, result):
        self.result = result


class Continue(Command):
    def __init__(self, continue_fn, *args, **kwargs):
        self.continue_fn = continue_fn
        self.args = args
        self.kwargs = kwargs


# endregion



class Proc(process.ProcessStateMachine):
    def on_running(self):
        try:
            command = self._state._run()

            # Cancelling takes precedence over everything else
            if self.cancelling:
                command = self.cancelling
            elif not isinstance(command, Command):
                command = Stop(command)

            if isinstance(command, Cancel):
                next_state = Cancelled(self.process, command.msg)
            else:
                if isinstance(command, Stop):
                    next_state = DONE(self.process, command.result)
                elif isinstance(command, Wait):
                    next_state = Waiting(
                        self.process, command.continue_fn, command.desc
                    )
                elif isinstance(command, Continue):
                    next_state = Running(
                        self.process, command.continue_fn, *command.args
                    )

                if self.pausing:
                    next_state = Paused(self.process, next_state)

        except BaseException as e:
            next_state = Failed(self.process, e)

        self.process._transition(next_state)
