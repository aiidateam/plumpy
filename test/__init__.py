"""Plumpy tests"""

from __future__ import absolute_import
import logging
import tempfile
import os

LOGFILE = os.path.join(tempfile.gettempdir(), 'plumpy_unittest.log')
try:
    os.remove(LOGFILE)
except OSError:
    pass
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)s()] %(message)s"
logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, format=FORMAT)
