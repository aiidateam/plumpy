# -*- coding: utf-8 -*-

from __future__ import absolute_import
from setuptools import setup

__author__ = "Martin Uhrin"
__license__ = "GPLv3 and MIT, see LICENSE file"
__contributors__ = "Sebastiaan Huber, Leopold Talirz, Dominik Gresch"

about = {}
with open('plumpy/version.py') as f:
    exec(f.read(), about)

setup(
    name="plumpy",
    version=about['__version__'],
    description='A python workflow library',
    long_description=open('README.rst').read(),
    url='https://github.com/muhrin/plumpy.git',
    author='Martin Uhrin',
    author_email='martin.uhrin@gmail.com',
    license=__license__,
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='workflow multithreaded rabbitmq',
    # Abstract dependencies.  Concrete versions are listed in
    # requirements.txt
    # See https://caremad.io/2013/07/setup-vs-requirement/ for an explanation
    # of the difference and
    # http://blog.miguelgrinberg.com/post/the-package-dependency-blues
    # for a useful dicussion
    install_requires=[
        'frozendict',
        'tornado>=4.1, <5.0',
        'kiwipy[rmq]>=0.3.4, <0.4.0',
        'enum34; python_version<"3.4"',
        'backports.tempfile; python_version<"3.2"',
        'six',
    ],
    extras_require={
        'dev': [
            'pip',
            'pytest',
            'ipython',
            'twine',
            'pytest-cov',
            'pre-commit',
            'shortuuid',
            'yapf',
            'prospector',
            'pylint<2; python_version<"3"',
            'pylint; python_version>="3"',
        ]
    },
    packages=['plumpy', 'plumpy/base'],
    test_suite='test')
