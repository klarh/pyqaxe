import os
import tempfile
import unittest

import pyqaxe as pyq

class DirectoryTests(unittest.TestCase):

    def test_restore(self):
        with tempfile.NamedTemporaryFile(suffix='.sqlite') as f:
            cache = pyq.Cache(f.name)
            (dirname, fname) = os.path.split(os.path.abspath(__file__))
            cache.index(pyq.mines.Directory(dirname))
            cache.close()

            cache = pyq.Cache(f.name)

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

    def test_relocate(self):
        with tempfile.TemporaryDirectory() as dirname:
            os.mkdir(os.path.join(dirname, 'test'))
            with open(os.path.join(dirname, 'test', 'test.txt'), 'w') as f:
                f.write('test text')

            cache = pyq.Cache(os.path.join(dirname, 'cache.sqlite'))
            cache.index(pyq.mines.Directory(
                dirname, relative=cache, exclude_suffixes=['sqlite', 'sqlite-journal']))
            cache.close()

            new_dirname = os.path.join(dirname, 'new_target')
            os.mkdir(new_dirname)
            os.rename(cache.location, os.path.join(new_dirname, 'cache.sqlite'))
            os.rename(os.path.join(dirname, 'test'), os.path.join(new_dirname, 'test'))

            new_cache = pyq.Cache(os.path.join(new_dirname, 'cache.sqlite'))

            size = 0
            for (size,) in new_cache.query('select count(*) from files'):
                pass

            self.assertEqual(size, 1)

            for row in new_cache.query('select * from files'):
                pass

            # make sure the file is actually readable in its new location
            with new_cache.open_file(row) as f:
                self.assertEqual(f.read(), 'test text')

    def test_relative(self):
        with tempfile.TemporaryDirectory() as dirname:
            os.mkdir(os.path.join(dirname, 'test'))
            with open(os.path.join(dirname, 'test', 'test.txt'), 'w') as f:
                f.write('test text')

            os.chdir(dirname)
            cache = pyq.Cache('cache.sqlite')
            cache.index(pyq.mines.Directory(
                '.', relative=True, exclude_suffixes=['sqlite', 'sqlite-journal']))
            cache.close()

            new_dirname = 'new_target'
            os.mkdir(new_dirname)
            os.rename(cache.location, os.path.join(new_dirname, 'cache.sqlite'))
            os.rename('test', os.path.join(new_dirname, 'test'))

            os.chdir(new_dirname)
            new_cache = pyq.Cache('cache.sqlite')

            size = 0
            for (size,) in new_cache.query('select count(*) from files'):
                pass

            self.assertEqual(size, 1)

            for row in new_cache.query('select * from files'):
                pass

            # make sure the file is actually readable in its new location
            with new_cache.open_file(row) as f:
                self.assertEqual(f.read(), 'test text')

if __name__ == '__main__':
    unittest.main()
