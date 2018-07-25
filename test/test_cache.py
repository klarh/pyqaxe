import os
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

if __name__ == '__main__':
    unittest.main()
