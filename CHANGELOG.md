# Changelog

## v0.18.3 - 2021-01-21

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
