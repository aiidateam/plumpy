Process
=======

Persistence
-----------

checkpoint

how to save and reserve from checkpoint

State machine
-------------

A process is also a `state machine<https://en.wikipedia.org/wiki/Finite-state_machine>`_ which can be in one of the following states:

* CREATED
* RUNNING
* WAITING
* FINISHED
* EXCEPTED
* KILLED

as defined in the :class:`ProcessState` enum.

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


How state transition
--------------------

Remote Controller
--------------------

control remote process (designate by pid)
