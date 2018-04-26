# -*- coding: utf-8 -*-

import abc
import collections
import inspect
import re
import sys

from . import mixins
from . import persistence
from . import processes
from . import process_states

__all__ = ['WorkChain', 'if_', 'while_', 'return_', 'ToContext', 'WorkChainSpec']

ToContext = dict


class WorkChainSpec(processes.ProcessSpec):
    def __init__(self):
        super(WorkChainSpec, self).__init__()
        self._outline = None

    def get_description(self):
        description = super(WorkChainSpec, self).get_description()

        if self._outline:
            description['outline'] = self._outline.get_description()

        return description

    def outline(self, *commands):
        """
        Define the outline that describes this work chain.

        :param commands: One or more functions that make up this work chain.
        """
        if len(commands) == 1:
            # There is only a single instruction
            self._outline = _ensure_instruction(commands[0])
        else:
            # There are multiple instructions
            self._outline = _Block(commands)

    def get_outline(self):
        return self._outline


@persistence.auto_persist('_awaiting')
class Waiting(process_states.Waiting):
    """ Overwrite the waiting state"""

    def __init__(self, process, done_callback, msg=None, awaiting=None):
        super(Waiting, self).__init__(process, done_callback, msg, awaiting)
        self._awaiting = {}
        for awaitable, key in awaiting.items():
            if isinstance(awaitable, processes.Process):
                awaitable = awaitable.future()
            self._awaiting[awaitable] = key

    def enter(self):
        super(Waiting, self).enter()
        for awaitable in self._awaiting.keys():
            awaitable.add_done_callback(self._awaitable_done)

    def exit(self):
        super(Waiting, self).exit()
        for awaitable in self._awaiting.keys():
            awaitable.remove_done_callback(self._awaitable_done)

    def _awaitable_done(self, awaitable):
        key = self._awaiting.pop(awaitable)
        try:
            self.process.ctx[key] = awaitable.result()
        except Exception as e:
            self._waiting_future.set_exception(e)
        else:
            if not self._awaiting:
                self._waiting_future.set_result(process_states.NULL)


class WorkChain(mixins.ContextMixin, processes.Process):
    """
    A WorkChain is a series of instructions carried out with the ability to save
    state in between.
    """
    _spec_type = WorkChainSpec
    _STEPPER_STATE = 'stepper_state'
    _CONTEXT = 'CONTEXT'

    @classmethod
    def get_state_classes(cls):
        states_map = super(WorkChain, cls).get_state_classes()
        states_map[process_states.ProcessState.WAITING] = Waiting
        return states_map

    def __init__(self, inputs=None, pid=None, logger=None, loop=None, communicator=None):
        super(WorkChain, self).__init__(inputs=inputs, pid=pid, logger=logger, loop=loop, communicator=communicator)
        self._stepper = None
        self._awaitables = {}

    def on_create(self):
        super(WorkChain, self).on_create()
        self._stepper = self.spec().get_outline().create_stepper(self)

    def save_instance_state(self, out_state, save_context):
        super(WorkChain, self).save_instance_state(out_state, save_context)

        # Ask the stepper to save itself
        if self._stepper is not None:
            out_state[self._STEPPER_STATE] = self._stepper.save()

    def load_instance_state(self, saved_state, load_context):
        super(WorkChain, self).load_instance_state(saved_state, load_context)

        # Recreate the stepper
        self._stepper = None
        stepper_state = saved_state.get(self._STEPPER_STATE, None)
        if stepper_state is not None:
            self._stepper = self.spec().get_outline().recreate_stepper(stepper_state, self)

    def to_context(self, **kwargs):
        """
        This is a convenience method that provides syntactic sugar, for
        a user to add multiple intersteps that will assign a certain value
        to the corresponding key in the context of the workchain
        """
        for key, awaitable in kwargs.items():
            if isinstance(awaitable, processes.Process):
                awaitable = awaitable.future()
            self._awaitables[awaitable] = key

    def run(self):
        return self._do_step()

    def _do_step(self):
        self._awaitables = {}

        try:
            finished, return_value = self._stepper.step()
        except _PropagateReturn as exception:
            finished, return_value = True, exception.exit_code

        if not finished and (return_value is None or isinstance(return_value, ToContext)):

            if isinstance(return_value, ToContext):
                self.to_context(**return_value)

            if self._awaitables:
                return process_states.Wait(self._do_step, 'Waiting before next step', self._awaitables)
            else:
                return process_states.Continue(self._do_step)
        else:
            return return_value


class Stepper(persistence.Savable):
    __metaclass__ = abc.ABCMeta

    def __init__(self, workchain):
        self._workchain = workchain

    def load_instance_state(self, saved_state, load_context):
        super(Stepper, self).load_instance_state(saved_state, load_context)
        self._workchain = load_context.workchain

    @abc.abstractmethod
    def step(self):
        """
        Execute on step of the instructions.
        :return: A 2-tuple with entries:
            0. True if the stepper has finished, False otherwise
            1. The return value from the executed step
        :rtype: tuple
        """
        pass


class _Instruction(object):
    """
    This class represents an instruction in a workchain. To step through the
    step you need to get a stepper by calling ``create_stepper()`` from which
    you can call the :class:`~Stepper.step()` method.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def create_stepper(self, workchain):
        """ Create a new stepper for this instruction """
        pass

    @abc.abstractmethod
    def recreate_stepper(self, saved_state, workchain):
        """ Recreate a stepper from a previously saved state """
        pass

    def __str__(self):
        return str(self.get_description())

    @abc.abstractmethod
    def get_description(self):
        """
        Get a text description of these instructions.
        :return: The description
        :rtype: dict or str
        """
        pass


class _FunctionStepper(Stepper):
    def __init__(self, workchain, fn):
        super(_FunctionStepper, self).__init__(workchain)
        self._fn = fn

    def save_instance_state(self, out_state, save_context):
        super(_FunctionStepper, self).save_instance_state(out_state, save_context)
        out_state['_fn'] = self._fn.__name__

    def load_instance_state(self, saved_state, load_context):
        super(_FunctionStepper, self).load_instance_state(saved_state, load_context)
        self._fn = getattr(self._workchain.__class__, saved_state['_fn'])

    def step(self):
        return True, self._fn(self._workchain)

    def __str__(self):
        return self._fn.__name__


class _FunctionCall(_Instruction):
    def __init__(self, func):
        try:
            args = inspect.getargspec(func)[0]
        except TypeError:
            raise TypeError("func is not a function, got {}".format(type(func)))
        if len(args) != 1:
            raise TypeError("Step must take one argument only: self")

        self._fn = func

    def create_stepper(self, workchain):
        return _FunctionStepper(workchain, self._fn)

    def recreate_stepper(self, saved_state, workchain):
        load_context = persistence.LoadSaveContext(workchain=workchain, func_spec=self)
        return _FunctionStepper.recreate_from(saved_state, load_context)

    def get_description(self):
        desc = self._fn.__name__
        if self._fn.__doc__:
            doc = re.sub(r'\n\s*', ' ', self._fn.__doc__).strip()
            desc += "({})".format(doc)

        return desc


STEPPER_STATE = 'stepper_state'


@persistence.auto_persist('_pos')
class _BlockStepper(Stepper):
    def __init__(self, block, workchain):
        super(_BlockStepper, self).__init__(workchain)
        self._block = block
        self._pos = 0
        self._child_stepper = self._block[0].create_stepper(self._workchain)

    def step(self):
        assert not self.finished(), "Can't call step after the block is finished"

        finished, result = self._child_stepper.step()
        if finished:
            self.next_instruction()

        return self.finished(), result

    def next_instruction(self):
        assert not self.finished()
        self._pos += 1
        if self.finished():
            self._child_stepper = None
        else:
            self._child_stepper = self._block[self._pos].create_stepper(self._workchain)

    def finished(self):
        return self._pos == len(self._block)

    def save_instance_state(self, out_state, save_context):
        super(_BlockStepper, self).save_instance_state(out_state, save_context)
        if self._child_stepper is not None:
            out_state[STEPPER_STATE] = self._child_stepper.save()

    def load_instance_state(self, saved_state, load_context):
        super(_BlockStepper, self).load_instance_state(saved_state, load_context)
        self._block = load_context.block_instruction
        stepper_state = saved_state.get(STEPPER_STATE, None)
        self._child_stepper = None
        if stepper_state is not None:
            self._child_stepper = self._block[self._pos].recreate_stepper(stepper_state, self._workchain)

    def __str__(self):
        return str(self._pos) + ':' + str(self._child_stepper)


class _Block(_Instruction, collections.Sequence):
    """
    Represents a block of instructions i.e. a sequential list of instructions.
    """

    def __init__(self, instructions):
        # Build up the list of commands
        comms = []
        for instruction in instructions:
            if not isinstance(instruction, _Instruction):
                # Assume it's a function call
                instruction = _FunctionCall(instruction)

            comms.append(instruction)
        self._instruction = comms

    def __getitem__(self, index):
        return self._instruction[index]

    def __len__(self):
        return len(self._instruction)

    def create_stepper(self, workchain):
        return _BlockStepper(self, workchain)

    def recreate_stepper(self, saved_state, workchain):
        load_context = persistence.LoadSaveContext(workchain=workchain, block_instruction=self)
        return _BlockStepper.recreate_from(saved_state, load_context)

    def get_description(self):
        return [instruction.get_description() for instruction in self._instruction]


class _Conditional(object):
    """
    Object that represents some condition with the corresponding body to be
    executed if the condition is met e.g.:
    if(condition):
      body

    or

    while(condition):
      body
    """

    def __init__(self, parent, predicate, label):
        self._parent = parent
        self._predicate = predicate
        self._body = None
        self._label = label

    @property
    def body(self):
        return self._body

    @property
    def predicate(self):
        return self._predicate

    def is_true(self, workflow):
        return self._predicate(workflow)

    def __call__(self, *instructions):
        assert self._body is None
        self._body = _Block(instructions)
        return self._parent

    def __str__(self):
        return self._label + '(' + self.predicate.__name__ + ')'


@persistence.auto_persist('_pos')
class _IfStepper(Stepper):
    def __init__(self, if_instruction, workchain):
        super(_IfStepper, self).__init__(workchain)
        self._if_instruction = if_instruction
        self._pos = 0
        self._child_stepper = None

    def step(self):
        if self.finished():
            return True, None

        if self._child_stepper is None:
            # Check the conditions until we find one that is true or we get to the end and
            # none are true in which case we set pos to past the end
            for conditional in self._if_instruction:
                if conditional.is_true(self._workchain):
                    break
                self._pos += 1

            if self.finished():
                return True, None
            else:
                self._child_stepper = self._if_instruction[self._pos].body.create_stepper(self._workchain)

        finished, retval = self._child_stepper.step()
        if finished:
            self._pos = len(self._if_instruction)
            self._child_stepper = None

        return self.finished(), retval

    def finished(self):
        return self._pos == len(self._if_instruction)

    def save_instance_state(self, out_state, save_context):
        super(_IfStepper, self).save_instance_state(out_state, save_context)
        if self._child_stepper is not None:
            out_state[STEPPER_STATE] = self._child_stepper.save()

    def load_instance_state(self, saved_state, load_context):
        super(_IfStepper, self).load_instance_state(saved_state, load_context)
        self._if_instruction = load_context.if_instruction
        stepper_state = saved_state.get(STEPPER_STATE, None)
        self._child_stepper = None
        if stepper_state is not None:
            self._child_stepper = self._if_instruction[self._pos].body.recreate_stepper(stepper_state, self._workchain)

    def __str__(self):
        s = str(self._if_instruction[self._pos])
        if self._child_stepper is not None:
            s += '(' + str(self._child_stepper) + ')'

        return s


class _If(_Instruction, collections.Sequence):
    def __init__(self, condition):
        super(_If, self).__init__()
        self._ifs = [_Conditional(self, condition, label=if_.__name__)]
        self._sealed = False

    def __getitem__(self, idx):
        return self._ifs[idx]

    def __len__(self):
        return len(self._ifs)

    def __call__(self, *commands):
        """
        This is how the commands for the if(...) body are set
        :param commands: The commands to run on the original if.
        :return: This instance.
        """
        self._ifs[0](*commands)
        return self

    def elif_(self, condition):
        self._ifs.append(_Conditional(self, condition, label=self.elif_.__name__))
        return self._ifs[-1]

    def else_(self, *commands):
        assert not self._sealed
        # Create a dummy conditional that always returns True
        cond = _Conditional(self, lambda wf: True, label=self.else_.__name__)
        cond(*commands)
        self._ifs.append(cond)
        # Can't do any more after the else
        self._sealed = True
        return self

    def create_stepper(self, workchain):
        return _IfStepper(self, workchain)

    def recreate_stepper(self, saved_state, workchain):
        load_context = persistence.LoadSaveContext(workchain=workchain, if_instruction=self)
        return _IfStepper.recreate_from(saved_state, load_context)

    def get_description(self):
        description = collections.OrderedDict()

        description['if({})'.format(self._ifs[0].predicate.__name__)] = self._ifs[0].body.get_description()
        for conditional in self._ifs[1:]:
            description['elif({})'.format(conditional.predicate.__name__)] = conditional.body.get_description()

        return description


class _WhileStepper(Stepper):
    def __init__(self, while_instruction, workchain):
        super(_WhileStepper, self).__init__(workchain)
        self._while_instruction = while_instruction
        self._child_stepper = None

    def step(self):
        # Do we need to check the condition?
        if self._child_stepper is None:
            # Should we go into the loop body?
            if self._while_instruction.is_true(self._workchain):
                self._child_stepper = self._while_instruction.body.create_stepper(self._workchain)
            else:  # Nope...we're done
                return True, None

        finished, result = self._child_stepper.step()
        if finished:
            self._child_stepper = None

        return False, result

    def save_instance_state(self, out_state, save_context):
        super(_WhileStepper, self).save_instance_state(out_state, save_context)
        if self._child_stepper is not None:
            out_state[STEPPER_STATE] = self._child_stepper.save()

    def load_instance_state(self, saved_state, load_context):
        super(_WhileStepper, self).load_instance_state(saved_state, load_context)
        self._while_instruction = load_context.while_instruction
        stepper_state = saved_state.get(STEPPER_STATE, None)
        self._child_stepper = None
        if stepper_state is not None:
            self._child_stepper = self._while_instruction.body.recreate_stepper(stepper_state, self._workchain)

    def __str__(self):
        s = str(self._while_instruction)
        if self._child_stepper is not None:
            s += '(' + str(self._child_stepper) + ')'

        return s


class _While(_Conditional, _Instruction, collections.Sequence):

    def __init__(self, predicate):
        super(_While, self).__init__(self, predicate, label=while_.__name__)

    def __getitem__(self, idx):
        assert idx == 0
        return self

    def __len__(self):
        return 1

    def create_stepper(self, workchain):
        return _WhileStepper(self, workchain)

    def recreate_stepper(self, saved_state, workchain):
        load_context = persistence.LoadSaveContext(workchain=workchain, while_instruction=self)
        return _WhileStepper.recreate_from(saved_state, load_context)

    def get_description(self):
        return {"while({})".format(self.predicate.__name__): self.body.get_description()}


class _PropagateReturn(BaseException):

    def __init__(self, exit_code):
        self.exit_code = exit_code


class _ReturnStepper(Stepper):

    def __init__(self, return_instruction, workchain):
        super(_ReturnStepper, self).__init__(workchain)
        self._return_instruction = return_instruction

    def step(self):
        """
        Raise a _PropagateReturn exception where the value is the exit code set
        in the _Return instruction upon instantiation
        """
        raise _PropagateReturn(self._return_instruction._exit_code)


class _Return(_Instruction):
    """
    A return instruction to tell the workchain to stop stepping through the
    outline and cease execution immediately.
    """

    def __init__(self, exit_code=None):
        super(_Return, self).__init__()
        self._exit_code = exit_code

    def __call__(self, exit_code):
        return _Return(exit_code)

    def create_stepper(self, workchain):
        return _ReturnStepper(self, workchain)

    def recreate_stepper(self, saved_state, workchain):
        return _ReturnStepper(self, workchain)

    def get_description(self):
        """
        Get a text description of these instructions.
        :return: The description
        :rtype: str
        """
        return 'Return from the outline immediately'


def if_(condition):
    """
    A conditional that can be used in a workchain outline.

    Use as::

      if_(cls.conditional)(
        cls.step1,
        cls.step2
      )

    Each step can, of course, also be any valid workchain step e.g. conditional.

    :param condition: The workchain method that will return True or False
    """
    return _If(condition)


def while_(condition):
    """
    A while loop that can be used in a workchain outline.

    Use as::

      while_(cls.conditional)(
        cls.step1,
        cls.step2
      )

    Each step can, of course, also be any valid workchain step e.g. conditional.

    :param condition: The workchain method that will return True or False
    """
    return _While(condition)


return_ = _Return()
"""
A global singleton that contains a Return instruction that allows to exit
out of the workchain outline directly with None as exit code
To set a specific exit code, call it with the desired exit code

Use as::

  if_(cls.conditional)(
    return_
  )

or

  if_(cls.conditional)(
    return_(EXIT_CODE)
  )

:param exit_code: an integer exit code to pass as the return value, None by default
"""


def _ensure_instruction(command):
    # There is only a single instruction
    if isinstance(command, _Instruction):
        return command
    else:
        # It must be a direct function call
        return _FunctionCall(command)
