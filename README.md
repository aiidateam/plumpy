# plumpy

[![Build status][github-ci]][github-link]
[![Docs status][rtd-badge]][rtd-link]
[![Latest Version][pypi-badge]][pypi-link]
[![PyVersions][pyversions-badge]][pyversions-link]
[![License][license-badge]][license-link]

A python workflows library that supports writing Processes with a well defined set of inputs and outputs that can be
strung together.

RabbitMQ is used to queue up, control and monitor running processes via the
[kiwipy](https://pypi.org/project/kiwipy/) library.

Features:

* Processes can be remotely controlled by sending messages over RabbitMQ all from a simple interface
* Progress can be saved between steps and continued later
* Optional explicit specification of inputs and outputs including their types, validation functions, help strings, etc.

## Installation

```bash
pip install plumpy
```

or

```bash
conda install -c conda-forge plumpy
```

## Development

This package utilises [tox](https://tox.readthedocs.io) for unit test automation, and [pre-commit](https://pre-commit.com/) for code style formatting and test automation.

To install these development dependencies:

```bash
pip install tox pre-commit
```

To run the unit tests:

```bash
tox
```

For the `rmq` tests you will require a running instance of RabbitMQ.
One way to achieve this is using Docker and launching [`test/rmq/docker-compose.yml`](test/rmq/docker-compose.yml).

To run the pre-commit tests:

```bash
pre-commit run --all
```

To build the documentation:

```bash
tox -e docs-clean
```

Changes should be submitted as Pull Requests (PRs) to the `develop` branch.

## Publishing Releases

1. Create a release PR/commit to the `develop` branch, updating `plumpy/version.py` and `CHANGELOG.md`.
2. Fast-forward merge `develop` into the `master` branch
3. Create a release on GitHub (<https://github.com/aiidateam/plumpy/releases/new>), pointing to the release commit on `master`, named `v.X.Y.Z` (identical to version in `plumpy/version.py`)
4. This will trigger the `continuous-deployment` GitHub workflow which, if all tests pass, will publish the package to PyPi. Check this has successfully completed in the GitHub Actions tab (<https://github.com/aiidateam/plumpy/actions>).

(if the release fails, delete the release and tag)

[github-ci]: https://github.com/aiidateam/plumpy/workflows/continuous-integration/badge.svg?branch=develop&event=push
[github-link]: https://github.com/aiidateam/plumpy/actions
[rtd-badge]: https://readthedocs.org/projects/plumpy/badge
[rtd-link]: http://plumpy.readthedocs.io/
[pypi-badge]: https://img.shields.io/pypi/v/plumpy.svg
[pypi-link]: https://pypi.python.org/pypi/plumpy/
[pyversions-badge]: https://img.shields.io/pypi/pyversions/plumpy.svg
[pyversions-link]: https://pypi.python.org/pypi/plumpy/
[license-badge]: https://img.shields.io/pypi/l/plumpy.svg
[license-link]: https://pypi.python.org/pypi/plumpy/
