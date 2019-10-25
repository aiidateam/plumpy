.. _quickstart:

Quickstart Examples
===================

Creating and running basis Process
------------------------------------

A Plumpy process can be create and run with:

1. Copy and paste the following code block into a new file called ``helloWorld.py``:

.. literalinclude:: ../../../examples/process_helloworld.py

2. run the process::

       (venv) $ python helloWorld.py

Process can wait, pause, play and resume
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The example below shows how process state transition with different action:

.. literalinclude:: ../../../examples/process_waitAndResume.py

Remote controled process
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

process start.

script to kill that process

Creating and running basis WorkChain
--------------------------------------

The WorkChain is a special process that can strung different small function
together into a independent process.

See the example below:

.. literalinclude:: ../../../examples/workchain_simple.py
