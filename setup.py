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
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
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
    python_requires='>=3.6',
    install_requires=[
        'pyyaml~=5.4', 'nest_asyncio~=1.4.0', 'aio-pika~=6.6', 'aiocontextvars~=0.2.2; python_version<"3.7"',
        'kiwipy[rmq]~=0.7.4'
    ],
    extras_require={
        'docs': ['sphinx~=3.2.0', 'myst-nb~=0.11.0', 'sphinx-book-theme~=0.0.39', 'ipython~=7.0'],
        'pre-commit': ['mypy==0.790', 'pre-commit~=2.2', 'pylint==2.5.2'],
        'tests': [
            'pytest==6.2.5', 'shortuuid==1.0.8', 'pytest-asyncio==0.16.0', 'pytest-cov==3.0.0',
            'pytest-notebook==0.7.0', 'ipykernel==6.12.1'
        ]
    },
    packages=['plumpy', 'plumpy/base'],
    include_package_data=True,
    test_suite='test'
)
