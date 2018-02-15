import functools

from . import class_loader as internal_class_loader
from . import communications
from . import futures

__all__ = ['ProcessReceiver', 'PAUSE_MSG', 'PLAY_MSG', 'KILL_MSG', 'STATUS_MSG',
           'ProcessAction', 'PlayAction', 'PauseAction', 'KillAction', 'StatusAction',
           'LaunchProcessAction', 'ContinueProcessAction', 'ExecuteProcessAction',
           'ProcessLauncher', 'create_continue_body', 'create_launch_body']

INTENT_KEY = 'intent'


class Intent(object):
    PLAY = 'play'
    PAUSE = 'pause'
    KILL = 'kill'
    STATUS = 'status'


PAUSE_MSG = {INTENT_KEY: Intent.PAUSE}
PLAY_MSG = {INTENT_KEY: Intent.PLAY}
KILL_MSG = {INTENT_KEY: Intent.KILL}
STATUS_MSG = {INTENT_KEY: Intent.STATUS}


class ProcessReceiver(object):
    """
    Responsible for receiving messages and translating them to actions
    on the process.
    """

    def __init__(self, process):
        """
        :param process: :class:`plumpy.Process`
        """
        self._process = process

    def __call__(self, msg):
        intent = msg['intent']
        if intent == Intent.PLAY:
            return self._process.play()
        elif intent == Intent.PAUSE:
            return self._process.pause()
        elif intent == Intent.KILL:
            return self._process.kill(msg=msg.get('msg', None))
        elif intent == Intent.STATUS:
            status_info = {}
            self._process.get_status_info(status_info)
            return status_info
        else:
            raise RuntimeError("Unknown intent")


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


class KillAction(ProcessAction):
    def __init__(self, pid):
        super(KillAction, self).__init__(pid, KILL_MSG)


TASK_KEY = 'task'
PLAY_KEY = 'play'
PERSIST_KEY = 'persist'
# Launch
PROCESS_CLASS_KEY = 'process_class'
ARGS_KEY = 'args'
KWARGS_KEY = 'kwargs'
NOWAIT_KEY = 'nowait'
# Continue
PID_KEY = 'pid'
TAG_KEY = 'tag'
# Task types
LAUNCH_TASK = 'launch'
CONTINUE_TASK = 'continue'


def create_launch_body(process_class, init_args=None, init_kwargs=None, play=True,
                       persist=False, nowait=True, class_loader=None):
    if class_loader is None:
        class_loader = internal_class_loader.get_class_loader()

    msg_body = {
        TASK_KEY: LAUNCH_TASK,
        PROCESS_CLASS_KEY: class_loader.class_identifier(process_class),
        PLAY_KEY: play,
        PERSIST_KEY: persist,
        NOWAIT_KEY: nowait,
    }
    if init_args:
        msg_body[ARGS_KEY] = init_args
    if init_kwargs:
        msg_body[KWARGS_KEY] = init_kwargs
    return msg_body


def create_continue_body(pid, tag=None, play=True, nowait=False):
    msg_body = {
        TASK_KEY: CONTINUE_TASK,
        PID_KEY: pid,
        PLAY_KEY: play,
        NOWAIT_KEY: nowait,
    }
    if tag is not None:
        msg_body[TAG_KEY] = tag
    return msg_body


class TaskAction(communications.Action):
    """ Action a task """

    def __init__(self, body):
        super(TaskAction, self).__init__()
        self._body = body

    def execute(self, publisher):
        future = publisher.task_send(self._body)
        futures.chain(future, self)


class LaunchProcessAction(TaskAction):
    def __init__(self, *args, **kwargs):
        """
        Calls through to create_launch_body to create the message and so has
        the same signature.
        """
        super(LaunchProcessAction, self).__init__(create_launch_body(*args, **kwargs))


class ContinueProcessAction(TaskAction):
    def __init__(self, *args, **kwargs):
        """
        Calls through to create_continue_body to create the message and so
        has the same signature.
        """
        super(ContinueProcessAction, self).__init__(create_continue_body(*args, **kwargs))


class ExecuteProcessAction(communications.Action):
    def __init__(self, process_class, init_args=None, init_kwargs=None, nowait=False, class_loader=None):
        super(ExecuteProcessAction, self).__init__()
        self._launch_action = LaunchProcessAction(
            process_class, init_args, init_kwargs, play=False, persist=True, class_loader=class_loader)
        self._nowait = nowait

    def get_launch_future(self):
        return self._launch_action

    def execute(self, publisher):
        self._launch_action.add_done_callback(
            functools.partial(self._on_launch_done, publisher))
        self._launch_action.execute(publisher)

    def _on_launch_done(self, publisher, launch_future):
        # The result of the launch future is the PID of the process
        if launch_future.cancelled():
            self.kill()
        elif launch_future.exception() is not None:
            self.set_exception(launch_future.exception())
        else:
            # Action the continue task
            continue_action = ContinueProcessAction(launch_future.result(), play=True, nowait=self._nowait)
            if self._nowait:
                continue_action.add_done_callback(
                    lambda x: self.set_result(launch_future.result()))
            else:
                futures.chain(continue_action, self)
            continue_action.execute(publisher)


class ProcessLauncher(object):
    """
    Takes incoming task messages and uses them to launch processes.

    Expected format of task:
    For launch:
    {
        'task': [LAUNCH_TASK]
        'process_class': [Process class to launch]
        'args': [tuple of positional args for process constructor]
        'kwargs': [dict of keyword args for process constructor].
        'nowait': True or False
    }

    For continue
    {
        'task': [CONTINUE_TASK]
        'pid': [Process ID]
        'nowait': True or False
    }
    """

    def __init__(self, loop=None, persister=None, load_context=None, class_loader=None):
        self._loop = loop
        self._persister = persister
        self._load_context = load_context

        if class_loader is not None:
            self._class_loader = class_loader
        else:
            self._class_loader = internal_class_loader.get_class_loader()

    def __call__(self, task):
        """
        Receive a task.
        :param task: The task message
        """
        task_type = task[TASK_KEY]
        if task_type == LAUNCH_TASK:
            return self._launch(task)
        elif task_type == CONTINUE_TASK:
            return self._continue(task)
        else:
            raise communications.TaskRejected

    def _launch(self, task):
        if task[PERSIST_KEY] and not self._persister:
            raise communications.TaskRejected("Cannot persist process, no persister")

        proc_class = self._class_loader.load_class(task[PROCESS_CLASS_KEY])
        args = task.get(ARGS_KEY, ())
        kwargs = task.get(KWARGS_KEY, {})
        proc = proc_class(*args, **kwargs)
        if task[PERSIST_KEY]:
            self._persister.save_checkpoint(proc)
        if task[PLAY_KEY]:
            proc.start()

        if task[NOWAIT_KEY]:
            return proc.pid
        else:
            return proc.future()

    def _continue(self, task):
        if not self._persister:
            raise communications.TaskRejected("Cannot continue process, no persister")

        tag = task.get(TAG_KEY, None)
        saved_state = self._persister.load_checkpoint(task[PID_KEY], tag)
        proc = saved_state.unbundle(self._load_context)

        # Call start in case it's not started yet
        proc.start()

        if task[NOWAIT_KEY]:
            return True
        else:
            return proc.future()
