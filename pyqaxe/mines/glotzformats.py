import glotzformats
import json
import sqlite3
from .. import Cache, util

def open_glotzformats(cache_id, file_row, suffix):
    open_mode = 'rb' if suffix in GlotzFormats.binary_formats else 'r'
    cache = Cache.get_opened_cache(cache_id)
    opened_file = cache.open_file(file_row, open_mode)
    trajectory = GlotzFormats.readers[suffix]().read(opened_file)
    return (opened_file, trajectory)

def close_glotzformats(args):
    (opened_file, _) = args
    opened_file.close()

def encode_glotzformats_data(file_id, cache_id, frame, attribute):
    return json.dumps([file_id, cache_id, frame, attribute]).encode('UTF-8')

def convert_glotzformats_data(contents):
    (file_id, cache_id, frame, attribute) = json.loads(contents.decode('UTF-8'))
    cache = Cache.get_opened_cache(cache_id)
    for row in cache.query('SELECT * from files WHERE rowid = ?', (file_id,)):
        # set row for open_file below
        pass

    suffix = row[0].split('.')[-1]

    (_, trajectory) = GlotzFormats.opened_trajectories_(cache_id, row, suffix)
    return getattr(trajectory[frame], attribute)

class GlotzFormats:
    """Expose frames of glotzformats-readable trajectory formats.

    `GlotzFormats` parses trajectory files and exposes them with a
    common interface. Files are opened once upon indexing to query the
    number of frames and data are read on-demand as frame data are
    selected.

    GlotzFormats objects create the following table in the database:

    - glotzformats_frames: Contains entries for each frame found in all trajectory files

    The **glotzformats_frames** table has the following columns:

    - file_id: files table identifier for the archive containing this record
    - cache_id: `Cache` unique identifier for the archive containing this record
    - frame: Integer (0-based) corresponding to the frame index within the trajectory
    - box: Glotzformats box object for the frame
    - types: Glotzformats types object for the frame
    - positions: Glotzformats positions object for the frame
    - velocities: Glotzformats velocities object for the frame
    - orientations: Glotzformats orientations object for the frame
    - shapedef: Glotzformats shapedef object for the frame

    .. note::
        Consult the glotzformats documentation to find more details
        about the encoding of the various data types listed here.

    """
    opened_trajectories_ = util.LRU_Cache(open_glotzformats, close_glotzformats, 16)

    binary_formats = {'zip', 'tar', 'sqlite', 'gsd'}

    readers = dict(
        zip=glotzformats.reader.GetarFileReader,
        tar=glotzformats.reader.GetarFileReader,
        sqlite=glotzformats.reader.GetarFileReader,
        pos=glotzformats.reader.PosFileReader,
        gsd=glotzformats.reader.GSDHOOMDFileReader
        )

    known_frame_attributes = ['box', 'types', 'positions', 'velocities',
                              'orientations', 'shapedef']

    def __init__(self):
        pass

    def index(self, cache, conn, mine_id=None, force=False):
        self.check_adapters()

        all_attributes = ', '.join(
            ['{} GLOTZFORMATS_{}'.format(attr, attr)
             for attr in self.known_frame_attributes])
        query = ('CREATE TABLE IF NOT EXISTS glotzformats_frames '
                 '(file_id INTEGER, cache_id TEXT, '
                 'frame INTEGER, {attributes}, '
                 'CONSTRAINT unique_glotzformats_path '
                 'UNIQUE (file_id, cache_id, frame) ON CONFLICT IGNORE)').format(
                     attributes=all_attributes)
        conn.execute(query)

        # don't do file IO if we aren't forced
        if not force:
            return

        # all rows to insert into glotzformats_frames (TODO interleave
        # reading and writing if size of all_values becomes an issue)
        all_values = []
        for row in conn.execute(
                'SELECT rowid, path, mine_id from files WHERE path LIKE "%.zip" OR '
                'path LIKE "%.tar" OR path LIKE "%.sqlite" OR '
                'path LIKE "%.pos" OR path LIKE "%.gsd"'):
            file_id = row[0]
            suffix = row[1].split('.')[-1]
            row = row[1:]

            (_, trajectory) = GlotzFormats.opened_trajectories_(cache.unique_id, row, suffix)
            for frame in range(len(trajectory)):
                values = [file_id, cache.unique_id, frame]
                for attr in self.known_frame_attributes:
                    values.append(encode_glotzformats_data(file_id, cache.unique_id, frame, attr))
                all_values.append(values)

        query = 'INSERT INTO glotzformats_frames VALUES ({})'.format(
            ', '.join((len(self.known_frame_attributes) + 3)*'?'))
        for values in all_values:
            conn.execute(query, values)

    @classmethod
    def check_adapters(cls):
        try:
            if cls.has_registered_adapters:
                return
        except AttributeError:
            # hasn't been registered yet, run the rest of this function
            pass

        for attr in cls.known_frame_attributes:
            upper_name = 'GLOTZFORMATS_{}'.format(attr.upper())
            sqlite3.register_converter(upper_name, convert_glotzformats_data)
        cls.has_registered_adapters = True

    def __getstate__(self):
        return []

    def __setstate__(self, state):
        self.__init__(*state)

    @classmethod
    def get_cache_size(cls):
        """Return the maximumnumber of files to keep open."""
        return cls.opened_trajectories_.max_size

    @classmethod
    def set_cache_size(cls, value):
        """Set the maximum number of files to keep open."""
        cls.opened_trajectories_.max_size = value
