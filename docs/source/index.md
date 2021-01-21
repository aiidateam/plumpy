# Plumpy

A python workflows library that supports writing Processes with a well defined set of inputs and outputs that can be chained together and nested.

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

or *via* Conda:

```console
$ conda install plumpy
```

or to work directly from the source code:

```console
$ git clone https://github.com/aiidateam/plumpy.git
$ cd plumpy
$ pip install -e ".[tests,docs]"
```

For remote controlled processes, you will also need to install and start the {{rabbitmq}} server.

## Getting Started

After you have successfully installed plumpy, you can walk-through the [user tutorial](./tutorial.ipynb) to help you on your way.

The design concepts behind plumpy can be found in [concepts section](./concepts.md), and the complete [API documentation](apidoc/plumpy.rst) is also provided.

```{toctree}
:hidden:

tutorial
concepts
API Reference <apidoc/plumpy>
_changelog
```

## Indices and tables

- {ref}`genindex`
- {ref}`search`

## Versioning

This software follows [Semantic Versioning](http://semver.org/)
