.. _concepts:

Concepts
========

Process
-------

The probably most basic concept in plumpy is the process. A process is an asynchronously running component that is typically defined as a static, "long-running" part of the workflow.

...

State Machine
~~~~~~~~~~~~~


A process can be in one of the following states:

* CREATED
* RUNNING
* WAITING
* FINISHED
* EXCEPTED
* KILLED

as defined in the :class:`~plumpy.process_states.ProcessState` enum.

::

                      ___
                     |   v
    CREATED (x) --- RUNNING (x) --- FINISHED (o)
                     |   ^          /
                     v   |         /
                    WAITING (x) --
                     |   ^
                      ---

    * -- EXCEPTED (o)
    * -- KILLED (o)

* (o): terminal state
* (x): non terminal state

Workchain
---------

A ``WorkChain`` is a sub-class of `Process`, which additionally defines a ``outline`` in its process specification.
This is a series of instructions carried out with the ability to save state in between.

The `outline` can give a succinct summary of the logical steps that the workchain will perform.
WorkChain supporting using `If_` and `While_` to control the state flow of certain processes.
