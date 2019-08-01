import json
import os
import tarfile
import tempfile
import unittest

import pyqaxe as pyq

try:
    import numpy as np
    import garnett
    from pyqaxe.mines.garnett import Garnett
except ImportError:
    np = garnett = Garnett = None

@unittest.skipIf(garnett is None, "Failed to import numpy or garnett")
class GarnettTests(unittest.TestCase):
    NUM_FRAMES = 3

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()

        positions = np.array([[1, 2, 3],
                              [-1, 2, 3]], dtype=np.float32)

        frames = []
        for i in range(cls.NUM_FRAMES):
            frame = garnett.trajectory.Frame()
            frame.frame_data = garnett.trajectory.FrameData()
            frame.frame_data.positions = positions.copy() + i
            frame.frame_data.box = garnett.trajectory.Box(10, 10, 10)
            frame.frame_data.types = ['A']*len(positions)
            frame.frame_data.shapedef = dict(A='sphere 1.0 005984FF')
            frame.frame_data.orientations = np.zeros((len(positions), 4))
            frames.append(frame)
        traj = garnett.trajectory.Trajectory(frames)

        posname = os.path.join(cls.temp_dir.name, 'test.pos')
        with open(posname, 'w') as f:
            garnett.writer.PosFileWriter().write(traj, file=f)

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
            cache.index(Garnett())
            cache.close()

            cache = pyq.Cache(f.name)

    def test_read_data(self):
        cache = pyq.Cache()
        cache.index(pyq.mines.Directory(self.temp_dir.name))
        cache.index(Garnett(exclude_suffixes=['tar']))

        count = 0
        for (positions,) in cache.query('select positions from garnett_frames'):
            count += 1

        self.assertEqual(count, self.NUM_FRAMES)

    def test_inside_tarfile(self):
        cache = pyq.Cache()
        cache.index(pyq.mines.TarFile(self.tarfile_name))
        cache.index(Garnett(exclude_regexes=['.*\.tar']))

        count = 0
        for (positions,) in cache.query('select positions from garnett_frames'):
            count += 1

        self.assertEqual(count, self.NUM_FRAMES)

if __name__ == '__main__':
    unittest.main()
