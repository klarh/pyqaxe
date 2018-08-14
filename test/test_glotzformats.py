import json
import os
import tarfile
import tempfile
import unittest

import pyqaxe as pyq

try:
    import numpy as np
    import glotzformats
    from pyqaxe.mines.glotzformats import GlotzFormats
except ImportError:
    np = glotzformats = GlotzFormats = None

@unittest.skipIf(glotzformats is None, "Failed to import numpy or glotzformats")
class GlotzFormatsTests(unittest.TestCase):
    NUM_FRAMES = 3

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()

        positions = np.array([[1, 2, 3],
                              [-1, 2, 3]], dtype=np.float32)

        frames = []
        for i in range(cls.NUM_FRAMES):
            frame = glotzformats.trajectory.Frame()
            frame.frame_data = glotzformats.trajectory.FrameData()
            frame.frame_data.positions = positions.copy() + i
            frame.frame_data.box = glotzformats.trajectory.Box(10, 10, 10)
            frame.frame_data.types = ['A']*len(positions)
            frame.frame_data.orientations = np.zeros((len(positions), 4))
            frames.append(frame)
        traj = glotzformats.trajectory.Trajectory(frames)

        posname = os.path.join(cls.temp_dir.name, 'test.pos')
        with open(posname, 'w') as f:
            glotzformats.writer.PosFileWriter().write(traj, file=f)

        cls.tarfile_name = os.path.join(cls.temp_dir.name, 'container.tar')
        with tarfile.open(cls.tarfile_name, 'w') as tf:
            tf.add(posname, arcname='test.pos')

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def test_restore(self):
        with tempfile.NamedTemporaryFile(suffix='.sqlite') as f:
            cache = pyq.Cache(f.name)
            cache.index(pyq.mines.Directory(
                self.temp_dir.name, exclude_suffixes=['tar']))
            cache.index(GlotzFormats())
            cache.close()

            cache = pyq.Cache(f.name)

    def test_read_data(self):
        cache = pyq.Cache()
        cache.index(pyq.mines.Directory(self.temp_dir.name))
        cache.index(GlotzFormats(exclude_suffixes=['tar']))

        count = 0
        for (positions,) in cache.query('select positions from glotzformats_frames'):
            count += 1

        self.assertEqual(count, self.NUM_FRAMES)

    def test_inside_tarfile(self):
        cache = pyq.Cache()
        cache.index(pyq.mines.TarFile(self.tarfile_name))
        cache.index(GlotzFormats(exclude_regexes=['.*\.tar']))

        count = 0
        for (positions,) in cache.query('select positions from glotzformats_frames'):
            count += 1

        self.assertEqual(count, self.NUM_FRAMES)

if __name__ == '__main__':
    unittest.main()
