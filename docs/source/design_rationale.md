# Design Rationale

The following page contains explanation of key design rationale.

This rationale is complimentary to the published article: [*Workflows in AiiDA: Engineering a High-Throughput, Event-Based Engine for Robust and Modular Computational Workflows*](https://doi.org/10.1016/j.commatsci.2020.110086)

## Use of asynchronicity

Plumpy, and its interactions with AiiDA, implement a mixed functions/coroutines model,
whereby many of plumpy's actions are handled by coroutine calls allowing us to use `awaits` and `yields` to effectively have cooperative multitasking between processes.

An early design decision was made not to push asynchronous code up to the user.
This means that methods such as `WorkChain` steps are regular functions (that are called *via* a coroutine further up the call stack).

It is of note that, due to the fact that `asyncio` does not support re-entrancy [by design](https://stackoverflow.com/questions/19471967/tulip-asyncio-why-not-all-calls-be-async-and-specify-when-things-should-be-sync/20218758#20218758),
once a regular function is used that branch of the call stack is now "locked in" to reminaing synchronous.
