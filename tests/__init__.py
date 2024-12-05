# -*- coding: utf-8 -*-
"""Plumpy tests"""

import logging
import os
import tempfile

LOGFILE = os.path.join(tempfile.gettempdir(), 'plumpy_unittest.log')
try:
    os.remove(LOGFILE)
except OSError:
    pass
FORMAT = '[%(filename)s:%(lineno)s - %(funcName)s()] %(message)s'
logging.basicConfig(filename=LOGFILE, level=logging.DEBUG, format=FORMAT)
