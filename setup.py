# -*- coding: utf-8 -*-
from setuptools import setup

__author__ = 'Martin Uhrin'
__license__ = 'GPLv3 and MIT, see LICENSE file'
__contributors__ = 'Sebastiaan Huber, Jason Yu, Leopold Talirz, Dominik Gresch'

ABOUT = {}
with open('plumpy/version.py') as f:
    exec(f.read(), ABOUT)  # pylint: disable=exec-used

setup(
    name='plumpy',
    version=ABOUT['__version__'],
    description='A python workflow library',
    long_description=open('README.rst').read(),
    url='https://github.com/muhrin/plumpy.git',
    author='Martin Uhrin',
    author_email='martin.uhrin@gmail.com',
    license=__license__,
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    keywords='workflow multithreaded rabbitmq',
    python_requires='>=3.5',
    install_requires=[
        'frozendict~=1.2', 'pyyaml~=5.1.2', 'nest_asyncio~=1.4.0', 'aio-pika~=6.6', 'aiocontextvars~=0.2.2',
        'kiwipy[rmq]~=0.6.0'
    ],
    extras_require={
        'docs': [
            'Sphinx~=2.0',
            'sphinx-rtd-theme~=0.5.0',
        ],
        'pre-commit': ['mypy==0.790', 'pre-commit~=2.2', 'pylint==2.5.2'],
        'tests': ['pytest~=5.4', 'shortuuid', 'pytest-asyncio', 'pytest-cov']
    },
    packages=['plumpy', 'plumpy/base'],
    test_suite='test'
)
