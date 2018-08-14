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

        cls.nested_tar_name = os.path.join(cls.temp_dir.name, 'nested.tar')
        with gtar.GTAR(cls.nested_tar_name, 'w') as traj, \
             open(os.path.join(cls.temp_dir.name, 'test.zip'), 'rb') as inner:
            traj.writeBytes('test.zip', inner.read())

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def test_restore(self):
        with tempfile.NamedTemporaryFile(suffix='.sqlite') as f:
            cache = pyq.Cache(f.name)
            cache.index(pyq.mines.Directory(self.temp_dir.name))
            cache.index(GTAR())
            cache.close()

            cache = pyq.Cache(f.name)

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

    def test_mine_accessors(self):
        cache = pyq.Cache()
        cache.index(pyq.mines.Directory(self.temp_dir.name))
        gtar_object = cache.index(GTAR())

        constructed_list = [cache.named_mines['Directory'], gtar_object]
        self.assertEqual(constructed_list, cache.ordered_mines)

    def test_set_get_cache(self):
        cache = pyq.Cache()
        cache.index(pyq.mines.Directory(self.temp_dir.name))
        cache_owner = cache.index(GTAR())

        old_cache_size = cache_owner.get_cache_size()
        new_cache_size = 2*old_cache_size
        cache_owner.set_cache_size(new_cache_size)

        self.assertEqual(new_cache_size, cache_owner.get_cache_size())

    def test_inside_tar_archive(self):
        cache = pyq.Cache()
        cache.index(pyq.mines.TarFile(self.nested_tar_name))
        cache.index(GTAR())

        row = None
        for row in cache.query('select * from gtar_records'):
            pass

        self.assertNotEqual(row, None)

        found_paths = set(row[0] for row in
                          cache.query('select path from gtar_records'))

        self.assertIn('test.json', found_paths)

        for (path, data) in cache.query('select path, data from gtar_records'):
            if path == 'test.json':
                decoded_test_json = json.loads(data)

        self.assertEqual(decoded_test_json['b'], 4)

if __name__ == '__main__':
    unittest.main()
