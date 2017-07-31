import apricotpy
from enum import Enum
import sys
import logging
from . import utils


def _get_member_function(process, name):
    try:
        return getattr(process, name)
    except AttributeError:
        raise ValueError(
            "This process does not have a function with "
            "the name '{}' as expected from the wait on".format(name)
        )


class ProcessState(Enum):
    """
    The possible states that a :class:`Process` can be in.
    """
    CREATED = 0
    RUNNING = 1
    WAITING = 2
    STOPPED = 3
    FAILED = 4


class State(object):
    @classmethod
    def create_from(cls, process, saved_state):
        """
        Create the process state from a saved instance state

        :param process: The process this state belongs to
        :param saved_state: The saved instance state
        :return: The wait on with its state as it was when it was saved
        :rtype: This class type
        """
        state = cls.__new__(cls)
        state.load_instance_state(process, saved_state)
        return state

    def __init__(self, process):
        """
        :param process: The process
        :type process: :class:`plum.process.Process`
        :param: The process state lock
        :type: :class:`threading.Lock`
        """
        self._process = process
        self._execute_callback = None

    @property
    def label(self):
        return self.LABEL

    def enter(self, previous_state):
        self._process.log_with_pid(logging.DEBUG, "entering state '{}'".format(self.label))

    def execute(self):
        self._execute_callback = None
        self._process.log_with_pid(logging.DEBUG, "executing state '{}'".format(self.label))

    def exit(self):
        self._process.log_with_pid(logging.DEBUG, "exiting state '{}'".format(self.label))

    def abort(self):
        if self._execute_callback is not None:
            self._execute_callback.cancel()
            self._execute_callback = None

    def save_instance_state(self, out_state):
        out_state['class_name'] = utils.fullname(self)

    def load_instance_state(self, process, saved_state):
        self._process = process
        self._execute_callback = None

    def _schedule_execute(self):
        """Schedule execution of the current Process state"""
        self._execute_callback = \
            self._process.loop().call_soon(self._process._execute_state)


class Created(State):
    LABEL = ProcessState.CREATED

    def __init__(self, process):
        super(Created, self).__init__(process)

    def enter(self, previous_state):
        super(Created, self).enter(previous_state)
        self._process._on_create()
        self._schedule_execute()

    def execute(self):
        super(Created, self).execute()
        # Move to the running state
        self._process._set_state(Running(self._process, self._process.do_run))


_NO_RESULT = ()


class Running(State):
    LABEL = ProcessState.RUNNING
    NEXT_STEP = 'NEXT_STEP'
    RESULT = 'RESULT'

    @staticmethod
    def _is_wait_retval(retval):
        """
        Determine if the value provided is a valid Wait return value which consists
        of a 2-tuple of a WaitOn and a callback function (or None) to be called
        after the wait on is ready

        :param retval: The return value from a step to check
        :return: True if it is a valid wait object, False otherwise
        """
        return (isinstance(retval, tuple) and
                len(retval) == 2 and
                isinstance(retval[0], apricotpy.Awaitable))

    def __init__(self, process, exec_func, result=_NO_RESULT):
        """
        :param process: The process this state belongs to
        :type process: :class:`plum.process.Process`
        :param exec_func: The run function to call during this state
        :param result: The (optional) result from a waiting state
        """
        super(Running, self).__init__(process)
        self._next_step = exec_func
        self._result = result

    def enter(self, previous_state):
        super(Running, self).enter(previous_state)

        if previous_state is ProcessState.CREATED:
            self._process._on_start()
        elif previous_state is ProcessState.WAITING:
            self._process._on_resume()
        elif previous_state is ProcessState.RUNNING:
            pass
        else:
            raise RuntimeError("Cannot enter RUNNING from '{}'".format(previous_state))

        self._process._on_run()
        self._schedule_execute()

    def execute(self):
        super(Running, self).execute()

        args = []
        if self._result is not _NO_RESULT:
            args.append(self._result)

        # Run the next function
        try:
            retval = self._next_step(*args)
        except BaseException:
            self._process._set_state(Failed(self._process, sys.exc_info()))
        else:
            if Running._is_wait_retval(retval):
                awaitable, callback = retval
                self._process._set_state(Waiting(self._process, awaitable, callback))
            else:
                self._process._set_state(Stopped(self._process))

    def save_instance_state(self, out_state):
        super(Running, self).save_instance_state(out_state)
        out_state[self.NEXT_STEP] = self._next_step.__name__
        out_state[self.RESULT] = self._result

    def load_instance_state(self, process, saved_state):
        super(Running, self).load_instance_state(process, saved_state)
        self._next_step = _get_member_function(self._process, saved_state[self.NEXT_STEP])
        self._result = saved_state[self.RESULT]


class Waiting(State):
    LABEL = ProcessState.WAITING
    NEXT_STEP = 'NEXT_STEP'
    AWAITING = 'AWAITING'

    def __init__(self, process, awaiting, fn):
        """
        :param process: The process this state belongs to
        :type process: :class:`plum.process.Process`
        :param fn: A callback function for the next Running state after
            finished waiting.  Can be None in which case next state will be
            STOPPED.
        """
        super(Waiting, self).__init__(process)
        self._awaiting = awaiting
        self._next_step = fn
        self._await_cb_handle = None

    def enter(self, previous_state):
        super(Waiting, self).enter(previous_state)
        self._process._on_wait(self._awaiting)
        self._schedule_execute()

    def execute(self):
        super(Waiting, self).execute()
        self._await_cb_handle = self._awaiting.add_done_callback(self._await_done)

    def abort(self):
        super(Waiting, self).abort()
        if self._await_cb_handle is not None:
            self._await_cb_handle.cancel()
            self._await_cb_handle = None

    def save_instance_state(self, out_state):
        super(Waiting, self).save_instance_state(out_state)
        if self._next_step is None:
            out_state[self.NEXT_STEP] = None
        else:
            out_state[self.NEXT_STEP] = self._next_step.__name__
        out_state[self.AWAITING] = self._awaiting

    def load_instance_state(self, process, saved_state):
        super(Waiting, self).load_instance_state(process, saved_state)

        self._next_step = _get_member_function(self._process, saved_state[self.NEXT_STEP])
        self._awaiting = saved_state[self.AWAITING]
        self._await_cb_handle = None

    def awaiting(self):
        return self._awaiting

    def _await_done(self, awaitable):
        self._await_cb_handle = None

        if self._next_step is None:
            self._process._set_state(Stopped(self._process))
        else:
            self._process._set_state(Running(self._process, self._next_step, awaitable.result()))


class Stopped(State):
    LABEL = ProcessState.STOPPED

    def __init__(self, process, abort=False, abort_msg=None):
        super(Stopped, self).__init__(process)
        self._abort = abort
        self._abort_msg = abort_msg

    def enter(self, previous_state):
        super(Stopped, self).enter(previous_state)

        if self._abort:
            self._process._on_abort(self._abort_msg)
        elif previous_state in [ProcessState.RUNNING, ProcessState.WAITING]:
            self._process._on_finish()
        else:
            raise RuntimeError("Cannot enter STOPPED from '{}'".format(previous_state))

        self._process._on_stop(self._abort_msg)
        self._schedule_execute()

    def execute(self):
        super(Stopped, self).execute()
        self._process._terminate()
        if self._abort:
            self._process.cancel()
        else:
            self._process.set_result(self._process.outputs)

    def save_instance_state(self, out_state):
        super(Stopped, self).save_instance_state(out_state)
        out_state['abort'] = self._abort
        out_state['abort_msg'] = self._abort_msg

    def load_instance_state(self, process, saved_state):
        super(Stopped, self).load_instance_state(process, saved_state)
        self._abort = saved_state['abort']
        self._abort_msg = saved_state['abort_msg']

    def get_abort_msg(self):
        return self._abort_msg

    def get_aborted(self):
        return self._abort


class Failed(State):
    LABEL = ProcessState.FAILED

    def __init__(self, process, exc_info):
        super(Failed, self).__init__(process)
        self._exc_info = exc_info

    def enter(self, previous_state):
        super(Failed, self).enter(previous_state)
        try:
            self._process._on_fail(self._exc_info)
        except BaseException:
            import traceback
            self._process.log_with_pid(
                logging.ERROR, "exception entering failed state:\n{}".format(traceback.format_exc()))
        self._schedule_execute()

    def execute(self):
        super(Failed, self).execute()
        self._process._terminate()
        self._process.set_exception(self._exc_info[1])

    def save_instance_state(self, out_state):
        super(Failed, self).save_instance_state(out_state)

        exc_info = self._exc_info
        # Saving traceback can be problematic so don't bother, just store None
        out_state['exc_info'] = (exc_info[0], exc_info[1], None)

    def load_instance_state(self, process, saved_state):
        super(Failed, self).load_instance_state(process, saved_state)
        self._exc_info = saved_state['exc_info']

    def get_exc_info(self):
        return self._exc_info


def load_state(process, state_bundle):
    # Get the class using the class loader and instantiate it
    class_name = state_bundle['class_name']
    proc_class = utils.load_class(class_name)
    return proc_class.create_from(process, state_bundle)
