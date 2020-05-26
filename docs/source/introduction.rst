Introduction
============

The plumpy is a library used for create and control workflow. It is consist
of Process as the minimal component of workflow and tools to control and interact with it.
It support writing Process with well defined inputs and outputs and it can be
strung together to become a workflow.

The Process is the core component of plumpy.
The purpose of process is to running a user defined action (defined in the function)
from creating to ending with all its states recorded and interactive.
It can be saved and loaded in memory or in disk at different states so that
the process can be suspended and resumed from any point of time when it is running.
This feature makes process the constituent part of the workchain which runs series
of instructions and has the ability to save the states in between.
Different processes are able to run asynchronously with each other in an event loop,
so when a process is doing the IO jobs, it will not blocking other process from
running to the terminal state.

The process can be controlled by using the process controller. In production
environment, plumpy uses RemoteProcessThreadController to control the running
behaviors of process. The thread controller communicate with the process over
the thread communicator provided by kiwipy which can subscribe and send messages over RabbitMQ.
Therefore, even if the computer is terminated unexpectedly the messages transmit
over RabbitMQ is not lost which makes the intermediate state of process preserved and no need
to be run from scratch.

After the process is created, more callback functions can be added to the process
with ProcessListener. It support adding and running callback functions when the
process at specific states.

The inputs and outputs are collectively defined in one place when creating the
new process class which make it more clear to quickly get the functionality and
the about how to use the process with the desired inputs.

The process can be nested in other process and making a more versatile process.
Therefore, the processes can be strung to create a WorkChain which is also a process
that runs series instructions of each of its processes.

You can reference 'concept' section to quickly get the concepts of each component
or look into the details in their separate section.
