.. plumpy documentation master file, created by
   sphinx-quickstart on Tue May 14 13:41:41 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to plumpy's documentation!
==================================

A python workflows library that supports writing Processes with a well defined set of inputs and outputs that can be strung together.

RabbitMQ is used to queue up, control and monitor running processes via the kiwipy library.

Features:

- Process can be remotely controlled by sending messages over RabbitMQ all from a simple interface
- Process can be saved between steps and continued later
- Optional explicit specification of inputs and outputs including their types, validation functions, help strings, etc.

 * To install plumpy follow the instructions in the :ref:`installation section<installation>`
 * After you have successfully installed plumpy, you can find some tips in the :ref:`user guide section<quickstart>` to help you on your way
 * The design concepts behind plumpy can be found in :ref:`concepts section<concepts>`
 * If you want to develop your process and workchain, you'll find :ref:`Develop Process section<develop_process>` and :ref:`Develop WorkChain section<develop_workchain>` useful.
 * Use the complete :doc:`API reference<apidoc/plumpy>`, the :ref:`modindex` or the :ref:`genindex` to find code you're looking for

.. toctree::
   :caption: Getting Started
   :maxdepth: 2

   gettingStarted/install
   gettingStarted/quickStart

.. toctree::
   :caption: Basic
   :maxdepth: 2

   basic/introduction
   basic/concepts
   basic/process
   basic/workchain

.. toctree::
   :caption: Advanced
   :maxdepth: 2

   advanced/process
   advanced/workchain
   advanced/plumpyAPIprocess
   advanced/plumpyAPIworkchain
   advanced/plumpyAPIfsm
   apidoc/plumpy

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`
