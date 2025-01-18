# -*- coding: utf-8 -*-
__version__ = '0.24.0'

import logging

from .base.state_machine import TransitionFailed

# interfaces
from .controller import ProcessController
from .coordinator import Coordinator
from .events import (
    PlumpyEventLoopPolicy,
    get_event_loop,
    new_event_loop,
    reset_event_loop_policy,
    run_until_complete,
    set_event_loop,
    set_event_loop_policy,
)
from .exceptions import (
    ClosedError,
    CoordinatorConnectionError,
    CoordinatorTimeoutError,
    InvalidStateError,
    KilledError,
    PersistenceError,
    UnsuccessfulResult,
)
from .futures import CancellableAction, Future, capture_exceptions, create_task
from .loaders import DefaultObjectLoader, ObjectLoader, get_object_loader, set_object_loader
from .message import MessageBuilder, ProcessLauncher, create_continue_body, create_launch_body
from .persistence import (
    Bundle,
    InMemoryPersister,
    LoadSaveContext,
    PersistedCheckpoint,
    Persister,
    PicklePersister,
    Savable,
    SavableFuture,
    auto_persist,
)
from .ports import UNSPECIFIED, InputPort, OutputPort, Port, PortNamespace, PortValidationError
from .process_listener import ProcessListener
from .process_spec import ProcessSpec
from .process_states import (
    Continue,
    Created,
    Excepted,
    Finished,
    Interruption,
    Kill,
    Killed,
    KillInterruption,
    PauseInterruption,
    ProcessState,
    Running,
    Stop,
    Wait,
    Waiting,
)
from .processes import BundleKeys, Process
from .utils import AttributesDict
from .workchains import ToContext, WorkChain, WorkChainSpec, if_, return_, while_

__all__ = (
    # ports
    'UNSPECIFIED',
    # utils
    'AttributesDict',
    # persistence
    'Bundle',
    # processes
    'BundleKeys',
    # futures
    'CancellableAction',
    # exceptions
    'ClosedError',
    # process_states/States
    'Continue',
    # coordinator
    'Coordinator',
    'CoordinatorConnectionError',
    'CoordinatorTimeoutError',
    'Created',
    # loaders
    'DefaultObjectLoader',
    'Excepted',
    'Finished',
    'Future',
    'InMemoryPersister',
    'InputPort',
    'Interruption',
    'InvalidStateError',
    # process_states/Commands
    'Kill',
    'KillInterruption',
    'Killed',
    'KilledError',
    'LoadSaveContext',
    # message
    'MessageBuilder',
    'ObjectLoader',
    'OutputPort',
    'PauseInterruption',
    'PersistedCheckpoint',
    'PersistenceError',
    'Persister',
    'PicklePersister',
    # event
    'PlumpyEventLoopPolicy',
    'Port',
    'PortNamespace',
    'PortValidationError',
    'Process',
    # controller
    'ProcessController',
    'ProcessLauncher',
    # process_listener
    'ProcessListener',
    'ProcessSpec',
    'ProcessState',
    'Running',
    'Savable',
    'SavableFuture',
    'Stop',
    # workchain
    'ToContext',
    'TransitionFailed',
    'UnsuccessfulResult',
    'Wait',
    'Waiting',
    'WorkChain',
    'WorkChainSpec',
    'auto_persist',
    'capture_exceptions',
    'create_continue_body',
    'create_launch_body',
    'create_task',
    'get_event_loop',
    'get_object_loader',
    'if_',
    'new_event_loop',
    'reset_event_loop_policy',
    'return_',
    'run_until_complete',
    'set_event_loop',
    'set_event_loop_policy',
    'set_object_loader',
    'while_',
)


# Do this se we don't get the "No handlers could be found..." warnings that will be produced
# if a user of this library doesn't set any handlers. See
# https://docs.python.org/3.1/library/logging.html#library-config
# for more details
class NullHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        pass


logging.getLogger('plumpy').addHandler(NullHandler())
