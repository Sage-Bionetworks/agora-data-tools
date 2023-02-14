import unittest
import numpy as np

from agoradatatools.etl.load import NumpyEncoder

class TestNumpyEncoder(unittest.TestCase):
    def setUp(self):
        self.encoder = NumpyEncoder()

    def test_integer(self):
        result = self.encoder.default(np.int64(1))
        self.assertEqual(result, 1)

    def test_float(self):
        result = self.encoder.default(np.float64(1))
        self.assertEqual(result, 1.0)

    def test_float_with_small_value(self):
        result = self.encoder.default(np.float64(0.00000003))
        self.assertEqual(result, 0.00000003)

    def test_array(self):
        result = self.encoder.default(np.array([1,2,3]))
        self.assertEqual(result, [1,2,3])
