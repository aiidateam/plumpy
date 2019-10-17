.. _installation:

.. highlight:: console

Installation
============

This document describes how to prepare for and install plumpy. Note that plumpy better requires that the user use the package
inside of a Python `virtualenv`_. Instructions for installing and creating a Python virtual environment are provided
below.

.. _virtualenv: https://virtualenv.pypa.io/en/stable/

Preparing Your Python Runtime Environment
-----------------------------------------

Plumpy currently requires a virtualenv to be active to install.

If not already present, please install the latest Python ``virtualenv`` using pip_::

    $ sudo pip install virtualenv

And create a virtual environment called ``venv`` in your home directory::

    $ virtualenv ~/venv

.. _pip: https://pip.readthedocs.io/en/latest/installing/

Now that you've created your virtualenv, activate your virtual environment::

    $ source ~/venv/bin/activate

Basic Installation
------------------

    $ pip install plumpy

Now you're ready to run :ref:`your first plumpy process and workchain <quickstart>`!

Building from Source
--------------------

If developing with plumpy, you will need to build from source. This allows changes you
make to plumpy to be reflected immediately in your runtime environment.

First, clone the source::

   $ git clone https://github.com/aiidateam/plumpy.git

Then, create and activate a virtualenv::

   $ virtualenv venv
   $ . venv/bin/activate
   $ pip install "plumpy[dev,docs]"
