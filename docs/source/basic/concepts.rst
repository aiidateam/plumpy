.. _concepts:

Concepts
========

Process
-------

The most basic concept in plumpy is the process.
A process is an asynchronously running component that is typically defined as a static, "long-running" part of the workflow.

Workchain
---------

A `WorkChain` is a sub-class of `Process`, with additional features for running a process as a set of discrete steps (also known as instructions).

A concrete `WorkChain` is created with a series of instructions to be carried out, and has the ability to save the state of the process after each instruction has completed.

The set of instructions is defined in the `outline` method, which provides a succinct summary of the logical steps that the workchain will perform.

WorkChains support the use of logical constructs such as `If_` and `While_` to control the state flow of certain processes.

Controller
----------

The `Controller` controls the process by sending and recieving signals from the `RabbitMQ <https://www.rabbitmq.com/>`__ message broker, using the Python interface implemented in `kiwipy <https://kiwipy.readthedocs.io/>`__.
It can launch, pause, continue, kill and check status of the process.

There are two types of `Controller` implementation in plumpy; the synchronous (blocking) `RemoteProcessThreadController` and asynchronous (non-blocking) `RemoteProcessController`.

One thing need to be notice is that controller communicate with process over the communicator, which is implemented by kiwipy.
In production environment, only thread communicator is used, since thread communicator is running on a independent thread(event loop) and will not be blocked by sometimes long waiting in the process event loop.
In case of long time no responding in RabbitMQ will be considered a network connection is broken.

Here is an example of launching and pausing process by controller.
