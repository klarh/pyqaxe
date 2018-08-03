import os
import tarfile
import tempfile
import unittest

import pyqaxe as pyq

class TarFileTests(unittest.TestCase):
    file_contents = {
        'test1.txt': 'test 1',
        'test2.txt': 'test 2',
        'test3.txt': 'test 3 abcd'
    }

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()

        dirname = cls.temp_dir.name

        for fname in cls.file_contents:
            with open(os.path.join(dirname, fname), 'w') as f:
                f.write(cls.file_contents[fname])

        cls.example_tar_name = os.path.join(dirname, 'test.tar')
        filenames = os.listdir(dirname)
        with tarfile.open(cls.example_tar_name, 'w') as tf:
            for filename in filenames[:-1]:
                tf.add(os.path.join(dirname, filename), arcname=filename)

        cls.example_tar_name2 = os.path.join(dirname, 'test2.tar')
        with tarfile.open(cls.example_tar_name2, 'w') as tf:
            for filename in filenames[1:]:
                tf.add(os.path.join(dirname, filename), arcname=filename)

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def test_restore(self):
        with tempfile.NamedTemporaryFile(suffix='.sqlite') as f:
            cache = pyq.Cache(f.name)
            cache.index(pyq.mines.TarFile(self.example_tar_name))
            cache.close()

            cache = pyq.Cache(f.name)

    def test_basic_indexing(self):
        cache = pyq.Cache()
        cache.index(pyq.mines.TarFile(self.example_tar_name))

        run_count = 0
        for row in cache.query('select path, * from files where path like "%.txt"'):
            fname, row = row[0], row[1:]
            with open(os.path.join(self.temp_dir.name, fname), 'r') as f:
                self.assertEqual(f.read(), self.file_contents[fname])
            run_count += 1

        self.assertEqual(run_count, len(self.file_contents) - 1)

    def test_read(self):
        cache = pyq.Cache()
        cache.index(pyq.mines.TarFile(self.example_tar_name))

        run_count = 0
        for row in cache.query('select path, * from files where path like "%.txt"'):
            (fname, row) = row[0], row[1:]
            with cache.open_file(row) as f:
                read_contents = f.read()
                self.assertEqual(self.file_contents[fname], read_contents)
            run_count += 1

        self.assertEqual(run_count, len(self.file_contents) - 1)

    def test_with_directory(self):
        cache = pyq.Cache()
        cache.index(pyq.mines.Directory(self.temp_dir.name))
        cache.index(pyq.mines.TarFile())

        count = 0
        for (count,) in cache.query('select count(*) from files'):
            pass

        # two tar files, N plaintext files, and (N-1) plaintext files
        # for each tar file
        number_of_tarfiles = 2
        number_of_entries = (number_of_tarfiles + len(self.file_contents) +
                             number_of_tarfiles*(len(self.file_contents) - 1))
        self.assertEqual(count, number_of_entries)

if __name__ == '__main__':
    unittest.main()
