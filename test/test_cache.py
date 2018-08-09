import os
import sqlite3
import tempfile
import unittest

import pyqaxe as pyq

class CacheTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def test_persist(self):
        persistent_name = os.path.join(self.temp_dir.name, 'test.sqlite')
        cache = pyq.Cache(persistent_name)
        cache.index(pyq.mines.Directory(self.temp_dir.name, exclude_suffixes=['first']))
        cache.index(pyq.mines.Directory(self.temp_dir.name, exclude_suffixes=['second']))
        cache.close()
        cache2 = pyq.Cache(persistent_name)

        mines = [cache2.mines[k] for k in sorted(cache2.mines)]

        self.assertIn('first', mines[0].exclude_suffixes)
        self.assertIn('second', mines[1].exclude_suffixes)

    def test_readonly(self):
        with tempfile.NamedTemporaryFile(suffix='.sqlite') as f:
            # when first creating a cache file, we should get an error
            # in readonly mode
            with self.assertRaises(sqlite3.OperationalError):
                cache = pyq.Cache(f.name, readonly=True)

            cache = pyq.Cache(f.name)
            (dirname, fname) = os.path.split(os.path.abspath(__file__))
            cache.index(pyq.mines.Directory(dirname))

            # with readonly mode, we shouldn't have an exception
            # (tables have already been created)
            for (path,) in cache.query('select path from files limit 2'):
                cache2 = pyq.Cache(f.name, readonly=True)

            with self.assertRaises(sqlite3.OperationalError):
                for (path,) in cache.query('select path from files limit 2'):
                    cache2 = pyq.Cache(f.name)

if __name__ == '__main__':
    unittest.main()
