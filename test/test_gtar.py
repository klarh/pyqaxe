import json
import os
import tempfile
import unittest
import gtar

import pyqaxe as pyq
from pyqaxe.mines.gtar import GTAR


class GTARTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()

        positions = [[1, 2, 3],
                     [-1, 2, 3]]

        test_json = json.dumps(dict(a=1.3, b=4))

        with gtar.GTAR(os.path.join(cls.temp_dir.name, 'test.zip'), 'w') as traj:
            traj.writePath('frames/10/position.f32.ind', positions)
            traj.writeStr('test.json', test_json)

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def test_read_data(self):
        cache = pyq.Cache()
        cache.index(pyq.mines.Directory(self.temp_dir.name))
        cache.index(GTAR())

        found_paths = set(row[0] for row in
                          cache.query('select path from gtar_records'))

        self.assertIn('test.json', found_paths)

        for (path, data) in cache.query('select path, data from gtar_records'):
            if path == 'test.json':
                decoded_test_json = json.loads(data)

        self.assertEqual(decoded_test_json['b'], 4)

if __name__ == '__main__':
    unittest.main()
