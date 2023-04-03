# Changelog

## v0.21.6 - 2023-04-03

### Bug fixes
- `PortNamespace`: Make `dynamic` apply recursively [[#263]](https://github.com/aiidateam/plumpy/pull/263)
- Workchains: Turn exception into warning for incorrect return type in conditional predicates: any type that implements `__bool__` will be accepted [[#265]](https://github.com/aiidateam/plumpy/pull/265)


## v0.21.5 - 2023-03-14

### Bug fixes
- Workchains: Accept but deprecate conditional predicates returning `None` [[#261]](https://github.com/aiidateam/plumpy/pull/261)


## v0.21.4 - 2023-03-09

### Bug fixes
- Workchains: Raise if `if_/while_` predicate does not return boolean [[#259]](https://github.com/aiidateam/plumpy/pull/259)

### Dependencies
- Dependencies: Update pre-commit requirement `isort==5.12.0` [[#260]](https://github.com/aiidateam/plumpy/pull/260)


## v0.21.3 - 2022-12-07

### Bug fixes
- `PortNamespace`: Fix bug in valid type checking of dynamic namespaces [[#255]](https://github.com/aiidateam/plumpy/pull/255)


## v0.21.2 - 2022-11-29

### Bug fixes
`Process`: Ensure that the raw inputs are not mutated [[#251]](https://github.com/aiidateam/plumpy/pull/251)

### Dependencies
- Add support for Python 3.10 and 3.11 [[#254]](https://github.com/aiidateam/plumpy/pull/254)
- Update requirement `pytest-notebook>=0.8.1` [[#254]](https://github.com/aiidateam/plumpy/pull/254)
- Update requirement `pyyaml~=6.0` [[#254]](https://github.com/aiidateam/plumpy/pull/254)
- Update requirement `kiwipy[rmq]~=0.7.7` [[#254]](https://github.com/aiidateam/plumpy/pull/254)
- Update the `myst-nb` and `sphinx` requirements [[#253]](https://github.com/aiidateam/plumpy/pull/253)


## v0.21.1 - 2022-11-21

This is a backport of changes introduced in `v0.22.0`.

### Features
- `Process`: Add the `is_excepted` property [[#240]](https://github.com/aiidateam/plumpy/pull/240)

### Bug fixes
- `StateMachine`: transition directly to excepted if transition failed [[#240]](https://github.com/aiidateam/plumpy/pull/240)
- `Process`: Fix incorrect overriding of `transition_failed` [[#240]](https://github.com/aiidateam/plumpy/pull/240)


## v0.21.0 - 2022-04-08

### Bug fixes
- Fix UnboundLocalError in DefaultObjectLoader.load_object. [[#225]](https://github.com/aiidateam/plumpy/pull/225)

### Dependencies
- Drop support for Python 3.6. [[#228]](https://github.com/aiidateam/plumpy/pull/228)
- Pin Jinja2 and Markupsafe packages for docs builds. [[#228]](https://github.com/aiidateam/plumpy/pull/228)
- Update requirement `nest-asyncio~=1.5` [[#229]](https://github.com/aiidateam/plumpy/pull/229)
- Pin tests requirements to functional versions. [[#228]](https://github.com/aiidateam/plumpy/pull/228)

### Devops
- Adopt PEP 621 and move build spec to `pyproject.toml` [[#230]](https://github.com/aiidateam/plumpy/pull/230)
- Move package into the `src/` subdirectory [[#234]](https://github.com/aiidateam/plumpy/pull/234)
- Merge separate license files into one [[#232]](https://github.com/aiidateam/plumpy/pull/232)
- Add the `flynt` and `isort` pre-commit hooks [[#233]](https://github.com/aiidateam/plumpy/pull/233)
- Remove obsolete `release.sh` [[#231]](https://github.com/aiidateam/plumpy/pull/231)
- Update the continuous deployment workflow [[#235]](https://github.com/aiidateam/plumpy/pull/235)


## v0.20.0 - 2021-08-10

- 🔧 MAINTAIN: update requirement to `pyyaml~=5.4` (#221)
  The versions of `pyyaml` up to v5.4 contained severe security issues where the default loaders could be abused for arbitrary code execution.
  The default `FullLoader` was patched to no longer allow this behavior, but as a result, data sets that could be successfully deserialized with it, now will fail.
  This required using the unsafe `Loader` in for the deserialization of the exception state of a process.


## v0.19.0 - 2021-03-09

- ‼️ DEPRECATE: `Process.done` method:
  This method is a duplicate of `Process.has_terminated`, and is not used anywhere in plumpy (or aiida-core).

- 🐛 FIX: `Task.cancel` should not set state as `EXCEPTED`
  `asyncio.CancelledError` are generated when an async task is cancelled.
  In python 3.7 this exception class inherits from `Exception`, whereas in python 3.8+ it inherits from `BaseException`.
  This meant it python 3.7 it was being caught by `except Exception`, and setting the process state to `EXCEPTED`,
  whereas in python 3.8+ it was being re-raised to the caller.
  We now ensure in both versions it is re-raised (particularly because aiida-core currently relies on this behaviour).

- 👌 IMPROVE: Process broadcast subscriber
  Filter out `state_changed` broadcasts, and allow these to pass-through without generating a (costly) asynchronous task.
  Note this also required an update in the minimal kiwipy version, to `0.7.4`

## v0.18.6 - 2021-02-24

👌 IMPROVE: Catch state change broadcast timeout

When using an RMQ communicator, the broadcast can timeout on heavy loads to RMQ
(for example see <https://github.com/aiidateam/aiida-core/issues/4745>).
This broadcast is not critical to the running of the process,
and so a timeout should not except it.

Also ensure the process PID is included in all log messages.

## v0.18.5 - 2021-02-15

Minor improvements and bug fixes:

- 🐛 FIX: retrieve future exception on_killed
  The exception set on the future should be retrieved, otherwise it will be caught by the loop's exception handler.
- 🐛 FIX: Clean-up process event hooks:
  On Process close/cleanup event hooks are removed,
  in part to not persist cyclic dependencies of hooks <-> Process.
  Once a process is closed, it will also not raise an Exception if a hook tries to un-register itself (but has already been removed by the clean-up).
- 👌 IMPROVE: Add `Process.is_killing` property
- 👌 IMPROVE: remove RUNNING from allowed states of `resume`:
  Since there is no `resume` method implemented for the `Running` class.
- 🔧 MAINTAIN: Remove frozendict dependency

## v0.18.4 - 2021-01-21

Minor update, to add `py.typed` file to distribution, in accordance with [PEP-561](https://www.python.org/dev/peps/pep-0561/) [[#195]](https://github.com/aiidateam/plumpy/pull/195)

## v0.18.2 - 2021-01-21

### Changes

- Allow for dereferencing of saved instance state [[#191]](https://github.com/aiidateam/plumpy/pull/191)
- Add type checking to code base [[#180]](https://github.com/aiidateam/plumpy/pull/180)
- Improve documentation [[#190]](https://github.com/aiidateam/plumpy/pull/190)

## v0.18.1 - 2020-12-18

### Bug fixes

- Trigger application of nest patch in `set_event_loop_policy` to make it compatible with Jupyter notebooks [[#189]](https://github.com/aiidateam/plumpy/pull/189)

## v0.18.0 - 2020-19-09

### Changes

- Drop support for Python 3.5 [[#187]](https://github.com/aiidateam/plumpy/pull/187)

### Dependencies

- Dependencies: update requirement `kiwipy~=0.7.1` [[#184]](https://github.com/aiidateam/plumpy/pull/184)

## v0.17.1 - 2020-11-25

### Bug fixes

- Dependencies: only require `aiocontextvars` for Python < 3.7 [[#181]](https://github.com/aiidateam/plumpy/pull/181)

## v0.17.0 - 2020-11-13

### Changes

- Add support for Python 3.9 [[#176]](https://github.com/aiidateam/plumpy/pull/176)
- Make application of `nest_asyncio` patch explicit [[#179]](https://github.com/aiidateam/plumpy/pull/179)

### Bug fixes

- `Port`: do not call validator if unspecified and port not required [[#173]](https://github.com/aiidateam/plumpy/pull/173)

## v0.16.1 - 2020-09-04

### Changes

- Dependencies: relax the requirement on `aio-pika` to `aio-pika~=6.6`. [[#171]](https://github.com/aiidateam/plumpy/pull/171)

## v0.16.0 - 2020-08-15

### Changes

- Drop `tornado` as a dependency and replace it fully by `asyncio` [[#166]](https://github.com/aiidateam/plumpy/pull/166)

## v0.15.0 - 2020-06-16

### Changes

- Drop support for Python 2.7 [[#151]](https://github.com/aiidateam/plumpy/pull/151)

### Bug fixes

- `LoopCommunicator`: fix incorrect call through in `remove_broadcast_subscriber` [[#156]](https://github.com/aiidateam/plumpy/pull/156)
- `PortNamespace`: do not add empty optional port namespaces to parsed inputs in the `pre_process` method [[#143]](https://github.com/aiidateam/plumpy/pull/143)
- `PortNamespace`: do not set `dynamic=False` when `valid_type=None` [[#146]](https://github.com/aiidateam/plumpy/pull/146)
- `PortNamespace`: set `dynamic=True` if `valid_type` in constructor [[#145]](https://github.com/aiidateam/plumpy/pull/145)

### Developers

- Migrate CI from Travis to Github Actions [[#152]](https://github.com/aiidateam/plumpy/pull/152)

## v0.14.5 - 2020-01-22

### Features

- `Port`: add context argument to validator method [[#141]](https://github.com/aiidateam/plumpy/pull/141)

### Changes

- Remove unnecessary abstraction layer `ValueSpec` [[#141]](https://github.com/aiidateam/plumpy/pull/141)

## v0.14.4 - 2019-12-12

### Bug fixes

- `ProcessSpec`: do not set `_spec` attribute if an error is raised in `spec` call [[#136]](https://github.com/aiidateam/plumpy/pull/136)

## v0.14.3 - 2019-10-25

### Features

- Allow lambdas for `InputPort` default values[[#133]](https://github.com/aiidateam/plumpy/pull/133)

### Bug fixes

- `PortNamespace`: move namespace validator after port validation [[#129]](https://github.com/aiidateam/plumpy/pull/129)


## v0.14.2 - 2019-07-16

### Features

- `PortNamespace`: add the concept of a "lazy" namespace  [[#121]](https://github.com/aiidateam/plumpy/pull/121)

### Bug fixes

- `PortNamespace`: fix the implementation of `include` in `absorb` [[#120]](https://github.com/aiidateam/plumpy/pull/120)


## v0.14.1 - 2019-06-17

### Features

- `PortNamespace`: add support for nested exclude/include rules in `absorb` [[#116]](https://github.com/aiidateam/plumpy/pull/116)
- Add traceback when setting exception on an excepted `Future` [[#113]](https://github.com/aiidateam/plumpy/pull/113)

## v0.14.0

### Bug fixes

- Fix bug in process spec validation with default and validator [[#106]](https://github.com/aiidateam/plumpy/pull/106)
- Fix call of `Portnamespace.validator` [[#104]](https://github.com/aiidateam/plumpy/pull/104)
