import unittest
import numpy as np
import json

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

    def test_in_json_dumps(self):
        test_data = {
            'a': np.int64(1),
            'b': np.float64(1.0),
            'c': np.float64(0.00000003),
            'd': np.array([4, 5, 6]),
            'e': str(1), #test handling of type handled by json.JSONEncoder
        }
        expected = '{"a": 1, "b": 1.0, "c": 3e-08, "d": [4, 5, 6], "e": "1"}'
        result = json.dumps(test_data, cls=NumpyEncoder)
        assert result == expected
        self.assertEqual(result, expected)
