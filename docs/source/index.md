# Plumpy

A python workflows library that supports writing Processes with a well defined set of inputs and outputs that can be strung together.

{{ rabbitmq }} is used to queue up, control and monitor running processes *via* the {{ kiwipy }} library.

Features
:  - Process can be remotely controlled by sending messages over RabbitMQ all from a simple interface
   - Process can be saved between steps and continued later
   - Optional explicit specification of inputs and outputs including their types, validation functions, help strings, etc.

## Installation

It is recommended to install plumpy into a [virtual environment](https://virtualenv.pypa.io):

```console
$ pip install plumpy
```

or to work directly from the source code:

```console
$ git clone https://github.com/aiidateam/plumpy.git
$ cd plumpy
$ pip install -e ".[tests,docs]"
```

For remote controlled processes, you will also need to install and start the {{rabbitmq}} server.

## Getting Started

After you have successfully installed plumpy, you can try out some of the examples in [user guide section](basic/examples.md) to help you on your way.

The design concepts behind plumpy can be found in [concepts section](basic/concepts.rst), and the complete [API documentation](apidoc/plumpy.rst) is also provided.

```{toctree}
:caption: Basic
:hidden:

basic/introduction
basic/concepts
basic/process
basic/workchain
basic/controller
basic/examples
```

```{toctree}
:caption: Advanced
:hidden:

advanced/process
advanced/workchain
API Reference <apidoc/plumpy>
```

## Indices and tables

- {ref}`genindex`
- {ref}`search`

## Versioning

This software follows [Semantic Versioning](http://semver.org/)
