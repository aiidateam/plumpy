# -*- coding: utf-8 -*-

from setuptools import setup

__license__ = "GPLv3 and MIT, see LICENSE file"
__version__ = "0.9.1"
__contributors__ = "Martin Uhrin"

setup(
    name="plumpy",
    version=__version__,
    description='A python workflow library',
    long_description=open('README.md').read(),
    url='https://github.com/aiidateam/plumpy.git',
    author='Martin Uhrin',
    author_email='Martin Uhrin <martin.uhrin@epfl.ch>',
    license=__license__,
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],
    keywords='workflow multithreaded rabbitmq',
    # Abstract dependencies.  Concrete versions are listed in
    # requirements.txt
    # See https://caremad.io/2013/07/setup-vs-requirement/ for an explanation
    # of the difference and
    # http://blog.miguelgrinberg.com/post/the-package-dependency-blues
    # for a useful dicussion
    install_requires=[
        'apricotpy', 'frozendict', 'portalocker', 'future'
    ],
    extras_require={
        'rmq': ['pika'],
        ':python_version<"3.4"': ['enum34'],
        ':python_version<"3.2"': ['futures'],
    },
    packages=['plum', 'plum.persistence'],
    test_suite='test'
)
