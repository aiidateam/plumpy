import logging
import tempfile
import os

testfile = os.path.join(tempfile.gettempdir(), 'plum_unittest.log')
print("Logging test to '{}'".format(testfile))
logging.basicConfig(filename=testfile, level=logging.DEBUG)
