# -*- coding: utf-8 -*-
from setuptools import setup

__author__ = 'Martin Uhrin'
__license__ = 'GPLv3 and MIT, see LICENSE file'
__contributors__ = 'Sebastiaan Huber, Leopold Talirz, Dominik Gresch'

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
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords='workflow multithreaded rabbitmq',
    python_requires='>=3.5',
    install_requires=[
        'frozendict',
        'tornado>=4.1, <5.0',
        'pyyaml~=5.1.2',
        'pika>=1.0.0',
        'kiwipy[rmq]~=0.6.0',
    ],
    extras_require={
        'docs': [
            'Sphinx==1.8.4',
            'Pygments==2.3.1',
            'docutils==0.14',
            'sphinx-rtd-theme==0.4.3',
        ],
        'pre-commit': ['pre-commit~=2.2', 'pylint==2.5.2'],
        'tests': ['pytest~=5.4', 'shortuuid']
    },
    packages=['plumpy', 'plumpy/base'],
    test_suite='test'
)
