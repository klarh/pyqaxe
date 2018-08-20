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
            # in read_only mode
            with self.assertRaises(sqlite3.OperationalError):
                cache = pyq.Cache(f.name, read_only=True)

            cache = pyq.Cache(f.name)
            (dirname, fname) = os.path.split(os.path.abspath(__file__))
            cache.index(pyq.mines.Directory(dirname))

            # with read_only mode, we shouldn't have an exception
            # (tables have already been created)
            for (path,) in cache.query('select path from files limit 2'):
                cache2 = pyq.Cache(f.name, read_only=True)

            with self.assertRaises(sqlite3.OperationalError):
                for (path,) in cache.query('select path from files limit 2'):
                    cache2 = pyq.Cache(f.name)

    def test_context(self):
        with open(os.path.join(self.temp_dir.name, 'test_context.txt'), 'w') as f:
            f.write('Test text')

        with pyq.Cache() as cache:
            cache.index(pyq.mines.Directory(self.temp_dir.name))

            self.assertEqual(len(cache.opened_file_cache_), 0)
            for row in cache.query('select * from files where path like "%test_context.txt"'):
                fileobj = cache.open_file(row)
            self.assertEqual(len(cache.opened_file_cache_), 1)

        self.assertEqual(len(cache.opened_file_cache_), 0)

if __name__ == '__main__':
    unittest.main()
