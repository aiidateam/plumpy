plumpy
======

.. image:: https://travis-ci.org/muhrin/plumpy.svg
    :target: https://travis-ci.org/muhrin/plumpy
    :alt: Travis CI

.. image:: https://readthedocs.org/projects/plumpy/badge
    :target: http://plumpy.readthedocs.io/
    :alt: Docs status

.. image:: https://img.shields.io/pypi/v/plumpy.svg
    :target: https://pypi.python.org/pypi/plumpy/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/wheel/plumpy.svg
    :target: https://pypi.python.org/pypi/plumpy/

.. image:: https://img.shields.io/pypi/pyversions/plumpy.svg
    :target: https://pypi.python.org/pypi/plumpy/

.. image:: https://img.shields.io/pypi/l/plumpy.svg
    :target: https://pypi.python.org/pypi/plumpy/


A python workflows library that supports writing Processes with a well defined set of inputs and outputs that can be
strung together.

RabbitMQ is used to queue up, control and monitor running processes via the
`kiwipy <https://pypi.org/project/kiwipy/>`_ library.


Features:

* Processes can be remotely controlled by sending messages over RabbitMQ all from a simple interface
* Progress can be saved between steps and continued later
* Optional explicit specification of inputs and outputs including their types, validation functions, help strings, etc.
