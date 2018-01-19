import functools

from . import communications
from . import futures
from . import utils

__all__ = ['ProcessReceiver', 'PAUSE_MSG', 'PLAY_MSG', 'CANCEL_MSG', 'STATUS_MSG',
           'ProcessAction', 'PlayAction', 'PauseAction', 'CancelAction', 'StatusAction',
           'LaunchProcessAction', 'ContinueProcessAction', 'ExecuteProcessAction',
           'ProcessLauncher']

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


TASK_KEY = 'task'
PLAY_KEY = 'play'
PERSIST_KEY = 'persist'
# Launch
PROCESS_CLASS_KEY = 'process_class'
ARGS_KEY = 'args'
KWARGS_KEY = 'kwargs'
# Continue
PID_KEY = 'pid'
TAG_KEY = 'tag'
# Task types
LAUNCH_TASK = 'launch'
CONTINUE_TASK = 'continue'


def create_launch_body(process_class, init_args=None, init_kwargs=None, play=True,
                       persist=False):
    msg_body = {
        TASK_KEY: LAUNCH_TASK,
        PROCESS_CLASS_KEY: utils.class_name(process_class),
        PLAY_KEY: play,
        PERSIST_KEY: persist,
    }
    if init_args:
        msg_body[ARGS_KEY] = init_args
    if init_kwargs:
        msg_body[KWARGS_KEY] = init_kwargs
    return msg_body


def create_continue_body(pid, tag=None, play=True):
    msg_body = {
        TASK_KEY: CONTINUE_TASK,
        PID_KEY: pid,
        PLAY_KEY: play,
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
    def __init__(self, process_class, init_args=None, init_kwargs=None):
        super(ExecuteProcessAction, self).__init__()
        self._launch_action = LaunchProcessAction(
            process_class, init_args, init_kwargs, play=False, persist=True)

    def get_launch_future(self):
        return self._launch_action

    def execute(self, publisher):
        self._launch_action.add_done_callback(
            functools.partial(self._on_launch_done, publisher))
        self._launch_action.execute(publisher)

    def _on_launch_done(self, publisher, launch_future):
        if launch_future.cancelled():
            self.cancel()
        elif launch_future.exception() is not None:
            self.set_exception(launch_future.exception())
        else:
            # Action the continue task
            continue_action = ContinueProcessAction(launch_future.result(), play=True)
            futures.chain(continue_action, self)
            continue_action.execute(publisher)


class ProcessLauncher(communications.TaskReceiver):
    """
    Takes incoming task messages and uses them to launch processes.

    Expected format of task:
    For launch:
    {
        'task': [LAUNCH_TASK]
        'process_class': [Process class to launch]
        'args': [tuple of positional args for process constructor]
        'kwargs': [dict of keyword args for process constructor]
    }

    For continue
    {
        'task': [CONTINUE_TASK]
        'pid': [Process ID]
    }
    """

    def __init__(self,
                 loop=None,
                 persister=None,
                 unbunble_args=(),
                 unbunble_kwargs=None,
                 ):
        self._loop = loop
        self._persister = persister
        self._unbundle_args = unbunble_args
        self._unbundle_kwargs = unbunble_kwargs if unbunble_kwargs is not None else {}

    def on_task_received(self, task):
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

        proc_class = utils.load_object(task[PROCESS_CLASS_KEY])
        args = task.get(ARGS_KEY, ())
        kwargs = task.get(KWARGS_KEY, {})
        proc = proc_class(*args, **kwargs)
        if task[PERSIST_KEY]:
            self._persister.save_checkpoint(proc)
        if task[PLAY_KEY]:
            proc.play()
        return proc.pid

    def _continue(self, task):
        if not self._persister:
            raise communications.TaskRejected("Cannot continue process, no persister")

        tag = task.get(TAG_KEY, None)
        saved_state = self._persister.load_checkpoint(task[PID_KEY], tag)
        proc = saved_state.unbundle(*self._unbundle_args, **self._unbundle_kwargs)
        proc.play()
        return proc.future()
