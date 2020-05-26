.. _concepts:

Concepts
========

Process
-------

The probably most basic concept in plumpy is the process. A process is an asynchronously running component that is typically defined as a static, "long-running" part of the workflow.

Workchain
---------

`WorkChain` is `Process`, however, not only the `Process`.

A WorkChain is a series of instructions carried out with the ability to save state in between.

The `outline` can give a succinct summary of the logical steps that the workchain will perform.
WorkChain supporting using `If_` and `While_` to control the state flow of certain processes.


Controller
__________
The controller control the process by sending and processing signals over RabbitMQ with the implementation of kiwipy.
It can launch, pause, continue, kill and check status of the process.
There are two types of implementation of controller in plumpy, the asynchronous one `RemoteProcessController` and synchronous
one `RemoteProcessThreadController`. The difference of `RemoteProcessController` is that its methods are coroutines which are running without blocking other tasks.
One thing need to be notice is that controller communicate with process over the communicator, which is implemented by kiwipy.
In production environment, only thread communicator is used, since thread communicator is running on a independent thread(event loop) and will not be blocked by sometimes long waiting in the process event loop.
In case of long time no responding in RabbitMQ will be considered a network connection is broken.

Here is an example of launching and pausing process by controller.
