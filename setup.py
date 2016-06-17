# -*- coding: utf-8 -*-
import os

__license__ = "GPLv3 and MIT, see LICENSE file"
__version__ = "0.4.0"
__contributors__ = "Martin Uhrin"

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

root_folder = os.path.split(os.path.abspath(__file__))[0]
bin_folder = os.path.join(root_folder, 'scripts')
setup(
    name="plum",
    license=__license__,
    version=__version__,
    # Abstract dependencies.  Concrete versions are listed in
    # requirements.txt
    # See https://caremad.io/2013/07/setup-vs-requirement/ for an explanation
    # of the difference and
    # http://blog.miguelgrinberg.com/post/the-package-dependency-blues
    # for a useful dicussion
    install_requires=['enum34', 'futures', 'frozendict'],
    packages=find_packages(),
    long_description=open(os.path.join(root_folder, 'README.md')).read(),
)

