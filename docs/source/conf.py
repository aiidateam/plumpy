# -*- coding: utf-8 -*-
#
# Configuration file for the Sphinx documentation builder.
#
# This file does only contain a selection of the most common options. For a
# full list see the documentation:
# http://www.sphinx-doc.org/en/master/config

import filecmp
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import plumpy

# -- Project information -----------------------------------------------------

project = 'plumpy'
copyright = '2021, AiiDA Team'
author = 'Martin Uhrin, Sebastiaan Huber, Jason Eu, Chris Sewell'

# The short X.Y version.
version = '.'.join(plumpy.__version__.split('.')[:2])
# The full version, including alpha/beta/rc tags.
release = plumpy.__version__

# -- General configuration ---------------------------------------------------

master_doc = 'index'
language = None
extensions = [
    'myst_nb', 'sphinx.ext.autodoc', 'sphinx.ext.doctest', 'sphinx.ext.viewcode', 'sphinx.ext.intersphinx',
    'IPython.sphinxext.ipython_console_highlighting'
]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = None

intersphinx_mapping = {
    'python': ('https://docs.python.org/3.8', None),
    'kiwipy': ('https://kiwipy.readthedocs.io/en/latest/', None)
}

myst_enable_extensions = ['colon_fence', 'deflist', 'html_image', 'smartquotes', 'substitution']
myst_url_schemes = ('http', 'https', 'mailto')
myst_substitutions = {
    'rabbitmq': '[RabbitMQ](https://www.rabbitmq.com/)',
    'kiwipy': '[kiwipy](https://kiwipy.readthedocs.io)'
}
nb_execution_mode = 'cache'
nb_execution_show_tb = 'READTHEDOCS' in os.environ
nb_execution_timeout = 60

# Warnings to ignore when using the -n (nitpicky) option
# We should ignore any python built-in exception, for instance
nitpick_ignore = []

for line in open('nitpick-exceptions'):
    if line.strip() == '' or line.startswith('#'):
        continue
    dtype, target = line.split(None, 1)
    target = target.strip()
    nitpick_ignore.append((dtype, target))

# -- Options for HTML output -------------------------------------------------

html_static_path = ['_static']
html_theme = 'sphinx_book_theme'
html_logo = '_static/logo.svg'
html_favicon = '_static/logo.svg'
html_theme_options = {
    'home_page_in_toc': True,
    'repository_url': 'https://github.com/aiidateam/plumpy',
    'repository_branch': 'develop',
    'use_repository_button': True,
    'use_issues_button': True,
    'path_to_docs': 'docs',
    'use_edit_page_button': True,
    'extra_navbar': ''
}

# API Documentation


def run_apidoc(app):
    """Runs sphinx-apidoc when building the documentation.

    Needs to be done in conf.py in order to include the APIdoc in the
    build on readthedocs.

    See also https://github.com/rtfd/readthedocs.org/issues/1139
    """
    from sphinx.ext.apidoc import main
    from sphinx.util import logging

    logger = logging.getLogger('apidoc')

    source_dir = Path(os.path.abspath(__file__)).parent
    apidoc_dir = source_dir / 'apidoc'
    apidoc_dir.mkdir(exist_ok=True)
    package_dir = source_dir.parent.parent / 'src' / 'plumpy'

    # we write to a temporary folder first then only move across files that have changed
    # this ensures that document rebuilds are not triggered every time (due to change in file mtime)
    with tempfile.TemporaryDirectory() as tmpdirname:
        options = [
            '-o', tmpdirname,
            str(package_dir), '--private', '--force', '--module-first', '--separate', '--no-toc', '--maxdepth', '4',
            '-q'
        ]

        os.environ['SPHINX_APIDOC_OPTIONS'] = 'members,special-members,private-members,undoc-members,show-inheritance'
        main(options)

        for path in Path(tmpdirname).glob('*'):
            if not (apidoc_dir / path.name).exists() or not filecmp.cmp(path, apidoc_dir / path.name):
                logger.info(f'Writing: {apidoc_dir / path.name}')
                shutil.move(path, apidoc_dir / path.name)


def setup(app):
    app.connect('builder-inited', run_apidoc)
