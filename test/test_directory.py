import os
import unittest

import pyqaxe as pyq

class DirectoryTests(unittest.TestCase):

    def test_this_file(self):
        (dirname, fname) = os.path.split(os.path.abspath(__file__))

        cache = pyq.Cache()
        cache.index(pyq.mines.Directory(dirname))

        for (count,) in cache.query(
                'select count(*) from files where path like "%/" || ?', (fname,)):
            pass

        self.assertEqual(count, 1)

    def test_list_dir(self):
        (dirname, _) = os.path.split(os.path.abspath(__file__))

        cache = pyq.Cache()
        cache.index(pyq.mines.Directory(dirname))
        contents = sum((filenames for (_, _, filenames) in os.walk(dirname)), [])

        for (count,) in cache.query('select count(*) from files'):
            pass

        self.assertEqual(count, len(contents))

if __name__ == '__main__':
    unittest.main()
