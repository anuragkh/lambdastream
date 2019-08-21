import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import lambdastream


class StreamTestSuite(unittest.TestCase):

    def test_stream(self):
        assert True
