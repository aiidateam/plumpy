.. plumpy documentation master file, created by
   sphinx-quickstart on Tue May 14 13:41:41 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to plumpy's documentation!
==================================

A python workflows library that supports writing Processes with a well defined set of inputs and outputs that can be strung together.

RabbitMQ is used to queue up, control and monitor running processes via the kiwipy library.

Features:

- Processes can be remotely controlled by sending messages over RabbitMQ all from a simple interface
- Progress can be saved between steps and continued later
- Optional explicit specification of inputs and outputs including their types, validation functions, help strings, etc.

 * To install plumpy follow the instructions in the :ref:`installation section<installation>`
 * After you have successfully installed plumpy, you can find some tips in the :ref:`user guide section<started>` to help you on your way
 * The design concepts behind plumpy can be found in :ref:`concepts section<concepts>`
 * Use the complete :doc:`API reference<apidoc/plumpy>`, the :ref:`modindex` or the :ref:`genindex` to find code you're looking for

.. toctree::
   :caption: Getting Started
   :maxdepth: 2

   gettingStarted/install
   gettingStarted/quickStart

.. toctree::
   :caption: Basic
   :maxdepth: 2

   running/introduction
   running/concepts
   running/process
   running/workchain
   running/custom
   running/debugging

.. toctree::
   :caption: Advance
   :maxdepth: 2

   developing/process
   developing/workchain
   developing/plumpyAPIprocess
   developing/plumpyAPIworkchain
   developing/plumpyAPIfsm
   apidoc/plumpy

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`
