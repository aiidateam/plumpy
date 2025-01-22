# -*- coding: utf-8 -*-
from __future__ import annotations

import abc
import asyncio
import collections
import inspect
import logging
import re
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    MutableSequence,
    Optional,
    Protocol,
    Self,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
)

from plumpy import utils
from plumpy.base import state_machine
from plumpy.base.utils import call_with_super_check
from plumpy.coordinator import Coordinator
from plumpy.event_helper import EventHelper
from plumpy.exceptions import InvalidStateError
from plumpy.loaders import ObjectLoader
from plumpy.persistence import LoadSaveContext, Savable, auto_persist, auto_save, ensure_object_loader
from plumpy.process_listener import ProcessListener

from . import lang, persistence, process_spec, process_states, processes
from .utils import PID_TYPE, SAVED_STATE_TYPE, AttributesDict

ToContext = dict

PREDICATE_TYPE = Callable[['WorkChain'], bool]
WC_COMMAND_TYPE = Callable[['WorkChain'], Any]
EXIT_CODE_TYPE = int


class WorkChainSpec(process_spec.ProcessSpec):
    def __init__(self) -> None:
        super().__init__()
        self._outline: Optional[Union['_Instruction', '_FunctionCall']] = None

    def get_description(self) -> Dict[str, str]:
        description = super().get_description()

        if self._outline:
            description['outline'] = self._outline.get_description()

        return description

    def outline(self, *commands: Union['_Instruction', WC_COMMAND_TYPE]) -> None:
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

    def get_outline(self) -> Union['_Instruction', '_FunctionCall']:
        assert self._outline is not None, 'outline not yet loaded'
        return self._outline


# FIXME:  better use composition here
@persistence.auto_persist('_awaiting')
class Waiting(process_states.Waiting):
    """Overwrite the waiting state"""

    def __init__(
        self,
        process: 'WorkChain',
        done_callback: Optional[Callable[..., Any]],
        msg: Optional[str] = None,
        data: Optional[Dict[Union[asyncio.Future, processes.Process], str]] = None,
    ) -> None:
        super().__init__(process, done_callback, msg, data)
        self._awaiting: Dict[asyncio.Future, str] = {}
        for awaitable, key in (data or {}).items():
            resolved_awaitable = awaitable.future() if isinstance(awaitable, processes.Process) else awaitable
            self._awaiting[resolved_awaitable] = key

    def _awaitable_done(self, awaitable: asyncio.Future) -> None:
        key = self._awaiting.pop(awaitable)
        try:
            self.process.ctx[key] = awaitable.result()  # type: ignore
        except Exception as exception:
            self._waiting_future.set_exception(exception)
        else:
            if not self._awaiting:
                self._waiting_future.set_result(lang.NULL)

    def enter(self) -> None:
        for awaitable in self._awaiting:
            awaitable.add_done_callback(self._awaitable_done)

    def exit(self) -> None:
        if self.is_terminal:
            raise InvalidStateError(f'Cannot exit a terminal state {self.LABEL}')

        for awaitable in self._awaiting:
            awaitable.remove_done_callback(self._awaitable_done)


class WorkChain(processes.Process):
    """
    A WorkChain is a series of instructions carried out with the ability to save
    state in between.
    """

    _spec_class = WorkChainSpec
    _STEPPER_STATE = 'stepper_state'
    CONTEXT = 'CONTEXT'

    @classmethod
    def get_state_classes(cls) -> Dict[process_states.ProcessState, Type[state_machine.State]]:
        states_map = super().get_state_classes()
        states_map[process_states.ProcessState.WAITING] = Waiting
        return states_map

    def __init__(
        self,
        inputs: Optional[dict] = None,
        pid: Optional[PID_TYPE] = None,
        logger: Optional[logging.Logger] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        coordinator: Optional[Coordinator] = None,
    ) -> None:
        super().__init__(inputs=inputs, pid=pid, logger=logger, loop=loop, coordinator=coordinator)
        self._context: Optional[AttributesDict] = AttributesDict()
        self._stepper: Optional[Stepper] = None
        self._awaitables: Dict[Union[asyncio.Future, processes.Process], str] = {}

    @property
    def ctx(self) -> Optional[AttributesDict]:
        return self._context

    @classmethod
    def spec(cls) -> WorkChainSpec:
        return cast(WorkChainSpec, super().spec())

    def on_create(self) -> None:
        super().on_create()
        self._stepper = self.spec().get_outline().create_stepper(self)

    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        """
        Ask the process to save its current instance state.

        :param out_state: A bundle to save the state to
        :param save_context: The save context
        """
        out_state: SAVED_STATE_TYPE = auto_save(self, loader)

        if isinstance(self._state, persistence.Savable):
            out_state['_state'] = self._state.save()

        # Inputs/outputs
        if self.raw_inputs is not None:
            out_state[processes.BundleKeys.INPUTS_RAW] = self.encode_input_args(self.raw_inputs)

        if self.inputs is not None:
            out_state[processes.BundleKeys.INPUTS_PARSED] = self.encode_input_args(self.inputs)

        if self.outputs:
            out_state[processes.BundleKeys.OUTPUTS] = self.encode_input_args(self.outputs)

        # Ask the stepper to save itself
        if self._stepper is not None and isinstance(self._stepper, Savable):
            out_state[self._STEPPER_STATE] = self._stepper.save()

        if self._context is not None:
            out_state[self.CONTEXT] = self._context.__dict__

        return out_state

    @classmethod
    def recreate_from(
        cls,
        saved_state: SAVED_STATE_TYPE,
        load_context: Optional[persistence.LoadSaveContext] = None,
    ) -> Self:
        """Recreate a workchain from a saved state, passing any positional

        :param saved_state: The saved state to load from
        :param load_context: The load context to use
        :return: An instance of the object with its state loaded from the save state.

        """
        ### FIXME: dup from process.create_from
        load_context = ensure_object_loader(load_context, saved_state)
        proc = cls.__new__(cls)

        # XXX: load_instance_state
        # First make sure the state machine constructor is called
        state_machine.StateMachine.__init__(proc)

        proc._setup_event_hooks()

        # Runtime variables, set initial states
        proc._future = persistence.SavableFuture()
        proc._event_helper = EventHelper(ProcessListener)
        proc._logger = None
        proc._coordinator = None

        if 'loop' in load_context:
            proc._loop = load_context.loop
        else:
            proc._loop = asyncio.get_event_loop()

        proc._state = proc.recreate_state(saved_state['_state'])

        if 'coordinator' in load_context:
            proc._coordinator = load_context.coordinator

        if 'logger' in load_context:
            proc._logger = load_context.logger

        # Need to call this here as things downstream may rely on us having the runtime variable above
        persistence.load_auto_persist_params(proc, saved_state, load_context)

        # Inputs/outputs
        try:
            decoded = proc.decode_input_args(saved_state[processes.BundleKeys.INPUTS_RAW])
            proc._raw_inputs = utils.AttributesFrozendict(decoded)
        except KeyError:
            proc._raw_inputs = None

        try:
            decoded = proc.decode_input_args(saved_state[processes.BundleKeys.INPUTS_PARSED])
            proc._parsed_inputs = utils.AttributesFrozendict(decoded)
        except KeyError:
            proc._parsed_inputs = None

        try:
            decoded = proc.decode_input_args(saved_state[processes.BundleKeys.OUTPUTS])
            proc._outputs = decoded
        except KeyError:
            proc._outputs = {}
        ### UNTILHERE FIXME: dup from process.create_from

        # context mixin
        try:
            proc._context = AttributesDict(**saved_state[proc.CONTEXT])
        except KeyError:
            pass

        # end of context mixin

        # Recreate the stepper
        proc._stepper = None
        stepper_state = saved_state.get(proc._STEPPER_STATE, None)
        if stepper_state is not None:
            proc._stepper = proc.spec().get_outline().recreate_stepper(stepper_state, proc)

        call_with_super_check(proc.init)
        return proc

    def to_context(self, **kwargs: Union[asyncio.Future, processes.Process]) -> None:
        """
        This is a convenience method that provides syntactic sugar, for
        a user to add multiple intersteps that will assign a certain value
        to the corresponding key in the context of the workchain
        """
        for key, awaitable in kwargs.items():
            resolved_awaitable = awaitable.future() if isinstance(awaitable, processes.Process) else awaitable

            self._awaitables[resolved_awaitable] = key

    async def run(self) -> Any:
        return self._do_step()

    def _do_step(self) -> Any:
        assert self._stepper is not None
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

            return process_states.Continue(self._do_step)

        return return_value


# XXX: Stepper is also a Saver with `save` method.
class Stepper(Protocol):
    def step(self) -> Tuple[bool, Any]:
        """
        Execute on step of the instructions.
        :return: A 2-tuple with entries:
            0. True if the stepper has finished, False otherwise
            1. The return value from the executed step

        """
        ...


class _Instruction(metaclass=abc.ABCMeta):
    """
    This class represents an instruction in a workchain. To step through the
    step you need to get a stepper by calling ``create_stepper()`` from which
    you can call the :class:`~Stepper.step()` method.
    """

    @abc.abstractmethod
    def create_stepper(self, workchain: 'WorkChain') -> Stepper:
        """Create a new stepper for this instruction"""

    @abc.abstractmethod
    def recreate_stepper(self, saved_state: SAVED_STATE_TYPE, workchain: 'WorkChain') -> Stepper:
        """Recreate a stepper from a previously saved state"""

    def __str__(self) -> str:
        return str(self.get_description())

    @abc.abstractmethod
    def get_description(self) -> Any:
        """
        Get a text description of these instructions.
        :return: The description

        """


@auto_persist()
class _FunctionStepper:
    def __init__(self, workchain: 'WorkChain', fn: WC_COMMAND_TYPE):
        self._workchain = workchain
        self._fn = fn

    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = persistence.auto_save(self, loader)
        out_state['_fn'] = self._fn.__name__

        return out_state

    @classmethod
    def recreate_from(
        cls, saved_state: SAVED_STATE_TYPE, load_context: Optional[persistence.LoadSaveContext] = None
    ) -> Self:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = persistence.auto_load(cls, saved_state, load_context)
        obj._workchain = load_context.workchain
        obj._fn = getattr(obj._workchain.__class__, saved_state['_fn'])

        return obj

    def step(self) -> Tuple[bool, Any]:
        return True, self._fn(self._workchain)

    def __str__(self) -> str:
        return self._fn.__name__


class _FunctionCall(_Instruction):
    def __init__(self, func: WC_COMMAND_TYPE) -> None:
        try:
            args = inspect.getfullargspec(func)[0]
        except TypeError:
            raise TypeError(f'func is not a function, got {type(func)}')
        if len(args) != 1:
            raise TypeError('Step must take one argument only: self')

        self._fn = func

    def create_stepper(self, workchain: 'WorkChain') -> _FunctionStepper:
        return _FunctionStepper(workchain, self._fn)

    def recreate_stepper(self, saved_state: SAVED_STATE_TYPE, workchain: 'WorkChain') -> _FunctionStepper:
        load_context = persistence.LoadSaveContext(workchain=workchain, func_spec=self)
        return cast(_FunctionStepper, _FunctionStepper.recreate_from(saved_state, load_context))

    def get_description(self) -> str:
        desc = self._fn.__name__
        if self._fn.__doc__:
            doc = re.sub(r'\n\s*', ' ', self._fn.__doc__).strip()
            desc += f'({doc})'

        return desc


STEPPER_STATE = 'stepper_state'


@persistence.auto_persist('_pos')
class _BlockStepper:
    def __init__(self, block: Sequence[_Instruction], workchain: 'WorkChain') -> None:
        self._workchain = workchain
        self._block = block
        self._pos: int = 0
        self._child_stepper: Optional[Stepper] = self._block[0].create_stepper(self._workchain)

    def step(self) -> Tuple[bool, Any]:
        assert not self.finished() and self._child_stepper is not None, "Can't call step after the block is finished"

        finished, result = self._child_stepper.step()
        if finished:
            self.next_instruction()

        return self.finished(), result

    def next_instruction(self) -> None:
        assert not self.finished()
        self._pos += 1
        if self.finished():
            self._child_stepper = None
        else:
            self._child_stepper = self._block[self._pos].create_stepper(self._workchain)

    def finished(self) -> bool:
        return self._pos == len(self._block)

    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = persistence.auto_save(self, loader)
        if self._child_stepper is not None and isinstance(self._child_stepper, Savable):
            out_state[STEPPER_STATE] = self._child_stepper.save()

        return out_state

    @classmethod
    def recreate_from(cls, saved_state: SAVED_STATE_TYPE, load_context: Optional[LoadSaveContext] = None) -> Self:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = persistence.auto_load(cls, saved_state, load_context)
        obj._workchain = load_context.workchain
        obj._block = load_context.block_instruction
        stepper_state = saved_state.get(STEPPER_STATE, None)
        obj._child_stepper = None
        if stepper_state is not None:
            obj._child_stepper = obj._block[obj._pos].recreate_stepper(stepper_state, obj._workchain)

        return obj

    def __str__(self) -> str:
        return str(self._pos) + ':' + str(self._child_stepper)


class _Block(_Instruction, collections.abc.Sequence):
    """
    Represents a block of instructions i.e. a sequential list of instructions.
    """

    def __init__(self, instructions: Sequence[Union[_Instruction, WC_COMMAND_TYPE]]) -> None:
        # Build up the list of commands
        comms: MutableSequence[_Instruction | _FunctionCall] = []
        for instruction in instructions:
            if not isinstance(instruction, _Instruction):
                # Assume it's a function call
                comms.append(_FunctionCall(instruction))
            else:
                comms.append(instruction)

        self._instruction: MutableSequence[_Instruction | _FunctionCall] = comms

    def __getitem__(self, index: int) -> Union[_Instruction, _FunctionCall]:  # type: ignore
        return self._instruction[index]

    def __len__(self) -> int:
        return len(self._instruction)

    def create_stepper(self, workchain: 'WorkChain') -> _BlockStepper:
        return _BlockStepper(self, workchain)

    def recreate_stepper(self, saved_state: SAVED_STATE_TYPE, workchain: 'WorkChain') -> _BlockStepper:
        load_context = persistence.LoadSaveContext(workchain=workchain, block_instruction=self)
        return cast(_BlockStepper, _BlockStepper.recreate_from(saved_state, load_context))

    def get_description(self) -> List[str]:
        return [instruction.get_description() for instruction in self._instruction]


class _Conditional:
    """
    Object that represents some condition with the corresponding body to be
    executed if the condition is met e.g.:
    if(condition):
      body

    or

    while(condition):
      body
    """

    def __init__(self, parent: _Instruction, predicate: PREDICATE_TYPE, label: str) -> None:
        self._parent = parent
        self._predicate = predicate
        self._body: Optional[_Block] = None
        self._label = label

    @property
    def body(self) -> _Block:
        assert self._body is not None, 'Instructions have not yet been set'
        return self._body

    @property
    def predicate(self) -> PREDICATE_TYPE:
        return self._predicate

    def is_true(self, workflow: 'WorkChain') -> bool:
        result = self._predicate(workflow)

        if not hasattr(result, '__bool__'):
            import warnings

            warnings.warn(
                f'The conditional predicate `{self._predicate.__name__}` returned `{result}` which is not boolean-like.'
                ' The return value should be `True` or `False` or implement the `__bool__` method. This behavior is '
                'deprecated and will soon start raising an exception.',
                UserWarning,
            )

        return result

    def __call__(self, *instructions: Union[_Instruction, WC_COMMAND_TYPE]) -> _Instruction:
        assert self._body is None, 'Instructions have already been set'
        self._body = _Block(instructions)
        return self._parent

    def __str__(self) -> str:
        return self._label + '(' + self.predicate.__name__ + ')'


@persistence.auto_persist('_pos')
class _IfStepper:
    def __init__(self, if_instruction: '_If', workchain: 'WorkChain') -> None:
        self._workchain = workchain
        self._if_instruction = if_instruction
        self._pos = 0
        self._child_stepper: Optional[Stepper] = None

    def step(self) -> Tuple[bool, Any]:
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

            self._child_stepper = self._if_instruction[self._pos].body.create_stepper(self._workchain)
        assert self._child_stepper is not None
        finished, retval = self._child_stepper.step()
        if finished:
            self._pos = len(self._if_instruction)
            self._child_stepper = None

        return self.finished(), retval

    def finished(self) -> bool:
        return self._pos == len(self._if_instruction)

    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = persistence.auto_save(self, loader)
        if self._child_stepper is not None and isinstance(self._child_stepper, Savable):
            out_state[STEPPER_STATE] = self._child_stepper.save()

        return out_state

    @classmethod
    def recreate_from(cls, saved_state: SAVED_STATE_TYPE, load_context: Optional[LoadSaveContext] = None) -> Self:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = persistence.auto_load(cls, saved_state, load_context)
        obj._workchain = load_context.workchain
        obj._if_instruction = load_context.if_instruction
        stepper_state = saved_state.get(STEPPER_STATE, None)
        obj._child_stepper = None
        if stepper_state is not None:
            obj._child_stepper = obj._if_instruction[obj._pos].body.recreate_stepper(stepper_state, obj._workchain)
        return obj

    def __str__(self) -> str:
        string = str(self._if_instruction[self._pos])
        if self._child_stepper is not None:
            string += '(' + str(self._child_stepper) + ')'

        return string


class _If(_Instruction, collections.abc.Sequence):
    def __init__(self, condition: PREDICATE_TYPE) -> None:
        super().__init__()
        self._ifs: List[_Conditional] = [_Conditional(self, condition, label=if_.__name__)]
        self._sealed = False

    def __getitem__(self, idx: int) -> _Conditional:  # type: ignore
        return self._ifs[idx]

    def __len__(self) -> int:
        return len(self._ifs)

    def __call__(self, *commands: Union[_Instruction, WC_COMMAND_TYPE]) -> '_If':
        """
        This is how the commands for the if(...) body are set
        :param commands: The commands to run on the original if.
        :return: This instance.
        """
        self._ifs[0](*commands)
        return self

    def elif_(self, condition: PREDICATE_TYPE) -> _Conditional:
        self._ifs.append(_Conditional(self, condition, label=self.elif_.__name__))
        return self._ifs[-1]

    def else_(self, *commands: Union[_Instruction, WC_COMMAND_TYPE]) -> '_If':
        assert not self._sealed
        # Create a dummy conditional that always returns True
        cond = _Conditional(self, lambda wf: True, label=self.else_.__name__)
        cond(*commands)
        self._ifs.append(cond)
        # Can't do any more after the else
        self._sealed = True
        return self

    def create_stepper(self, workchain: 'WorkChain') -> _IfStepper:
        return _IfStepper(self, workchain)

    def recreate_stepper(self, saved_state: SAVED_STATE_TYPE, workchain: 'WorkChain') -> _IfStepper:
        load_context = persistence.LoadSaveContext(workchain=workchain, if_instruction=self)
        return cast(_IfStepper, _IfStepper.recreate_from(saved_state, load_context))

    def get_description(self) -> Mapping[str, Any]:
        description = collections.OrderedDict()

        description[f'if({self._ifs[0].predicate.__name__})'] = self._ifs[0].body.get_description()
        for conditional in self._ifs[1:]:
            description[f'elif({conditional.predicate.__name__})'] = conditional.body.get_description()

        return description


class _WhileStepper:
    def __init__(self, while_instruction: '_While', workchain: 'WorkChain') -> None:
        self._workchain = workchain
        self._while_instruction = while_instruction
        self._child_stepper: Optional[_BlockStepper] = None

    def step(self) -> Tuple[bool, Any]:
        # Do we need to check the condition?
        if self._child_stepper is None:
            # Should we go into the loop body?
            if self._while_instruction.is_true(self._workchain):
                self._child_stepper = self._while_instruction.body.create_stepper(self._workchain)
            else:  # Nope...we're done
                return True, None
        assert self._child_stepper is not None
        finished, result = self._child_stepper.step()
        if finished:
            self._child_stepper = None

        return False, result

    def save(self, loader: ObjectLoader | None = None) -> SAVED_STATE_TYPE:
        out_state: SAVED_STATE_TYPE = persistence.auto_save(self, loader)

        if self._child_stepper is not None:
            out_state[STEPPER_STATE] = self._child_stepper.save()

        return out_state

    @classmethod
    def recreate_from(
        cls, saved_state: SAVED_STATE_TYPE, load_context: Optional[persistence.LoadSaveContext] = None
    ) -> Self:
        """
        Recreate a :class:`Savable` from a saved state using an optional load context.

        :param saved_state: The saved state
        :param load_context: An optional load context

        :return: The recreated instance

        """
        load_context = ensure_object_loader(load_context, saved_state)
        obj = persistence.auto_load(cls, saved_state, load_context)
        obj._workchain = load_context.workchain
        obj._while_instruction = load_context.while_instruction
        stepper_state = saved_state.get(STEPPER_STATE, None)
        obj._child_stepper = None
        if stepper_state is not None:
            obj._child_stepper = obj._while_instruction.body.recreate_stepper(stepper_state, obj._workchain)
        return obj

    def __str__(self) -> str:
        string = str(self._while_instruction)
        if self._child_stepper is not None:
            string += '(' + str(self._child_stepper) + ')'

        return string


class _While(_Conditional, _Instruction, collections.abc.Sequence):
    def __init__(self, predicate: PREDICATE_TYPE) -> None:
        super().__init__(self, predicate, label=while_.__name__)

    def __getitem__(self, idx: int) -> '_While':  # type: ignore
        assert idx == 0
        return self

    def __len__(self) -> int:
        return 1

    def create_stepper(self, workchain: 'WorkChain') -> _WhileStepper:
        return _WhileStepper(self, workchain)

    def recreate_stepper(self, saved_state: SAVED_STATE_TYPE, workchain: 'WorkChain') -> _WhileStepper:
        load_context = persistence.LoadSaveContext(workchain=workchain, while_instruction=self)
        return cast(_WhileStepper, _WhileStepper.recreate_from(saved_state, load_context))

    def get_description(self) -> Dict[str, Any]:
        return {f'while({self.predicate.__name__})': self.body.get_description()}


class _PropagateReturn(BaseException):
    def __init__(self, exit_code: Optional[EXIT_CODE_TYPE]) -> None:
        super().__init__()
        self.exit_code = exit_code


@persistence.auto_persist()
class _ReturnStepper:
    def __init__(self, return_instruction: '_Return', workchain: 'WorkChain') -> None:
        self._workchain = workchain
        self._return_instruction = return_instruction

    def step(self) -> Tuple[bool, Any]:
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

    def __init__(self, exit_code: Optional[EXIT_CODE_TYPE] = None) -> None:
        super().__init__()
        self._exit_code = exit_code

    def __call__(self, exit_code: EXIT_CODE_TYPE) -> '_Return':
        return _Return(exit_code)

    def create_stepper(self, workchain: 'WorkChain') -> _ReturnStepper:
        return _ReturnStepper(self, workchain)

    def recreate_stepper(self, saved_state: SAVED_STATE_TYPE, workchain: 'WorkChain') -> _ReturnStepper:
        return _ReturnStepper(self, workchain)

    def get_description(self) -> str:
        """
        Get a text description of these instructions.
        :return: The description

        """
        return 'Return from the outline immediately'


def if_(condition: PREDICATE_TYPE) -> _If:
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


def while_(condition: PREDICATE_TYPE) -> _While:
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

or::

  if_(cls.conditional)(
    return_(EXIT_CODE)
  )

:param exit_code: an integer exit code to pass as the return value, None by default
"""


def _ensure_instruction(command: Any) -> Union[_Instruction, _FunctionCall]:
    # There is only a single instruction
    if isinstance(command, _Instruction):
        return command

    # It must be a direct function call
    return _FunctionCall(command)
