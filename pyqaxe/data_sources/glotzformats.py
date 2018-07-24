import glotzformats
import json
import sqlite3
from .. import Cache

def encode_glotzformats_data(file_id, cache_id, frame, attribute):
    return json.dumps([file_id, cache_id, frame, attribute]).encode('UTF-8')

def convert_glotzformats_data(contents):
    (file_id, cache_id, frame, attribute) = json.loads(contents.decode('UTF-8'))
    cache = Cache.get_opened_cache(cache_id)
    for row in cache.query('SELECT * from files WHERE rowid = ?', (file_id,)):
        # set row for open_file below
        pass

    suffix = row[0].split('.')[-1]
    open_mode = 'rb' if suffix in GlotzFormats.binary_formats else 'r'

    # TODO use a cache to save on re-opening files each time
    with cache.open_file(row, open_mode) as f:
        trajectory = GlotzFormats.readers[suffix]().read(f)
        return getattr(trajectory[frame], attribute)

class GlotzFormats:
    # number of records to select at once
    select_limit = 128

    binary_formats = {'zip', 'tar', 'sqlite', 'gsd'}

    readers = dict(
        zip=glotzformats.reader.GetarFileReader,
        tar=glotzformats.reader.GetarFileReader,
        sqlite=glotzformats.reader.GetarFileReader,
        pos=glotzformats.reader.PosFileReader,
        gsd=glotzformats.reader.GSDHOOMDFileReader
        )

    known_frame_attributes = ['positions', 'orientations', 'box']

    def __init__(self):
        pass

    def index(self, cache, conn, data_source=None, force=False):
        self.check_adapters()

        conn.execute('CREATE TABLE IF NOT EXISTS glotzformats_frames '
                     '(file_id INTEGER, cache_id TEXT, '
                     'frame INTEGER, '
                     'positions GLOTZFORMATS_POSITIONS, '
                     'orientations GLOTZFORMATS_ORIENTATIONS, '
                     'box GLOTZFORMATS_BOX, '
                     'CONSTRAINT unique_glotzformats_path '
                     'UNIQUE (file_id, cache_id, frame) ON CONFLICT IGNORE)')

        # don't do file IO if we aren't forced
        if not force:
            return

        # all rows to insert into glotzformats_frames (TODO interleave
        # reading and writing if size of all_values becomes an issue)
        all_values = []
        for row in conn.execute(
                'SELECT rowid, path, data_source from files WHERE path LIKE "%.zip" OR '
                'path LIKE "%.tar" OR path LIKE "%.sqlite" OR '
                'path LIKE "%.pos" OR path LIKE "%.gsd"'):
            file_id = row[0]
            suffix = row[1].split('.')[-1]
            row = row[1:]

            open_mode = 'rb' if suffix in self.binary_formats else 'r'

            # TODO use a cache to save on re-opening files each time
            with cache.open_file(row, open_mode) as f:
                trajectory = self.readers[suffix]().read(f)
                for frame in range(len(trajectory)):
                    values = [file_id, cache.unique_id, frame]
                    for attr in self.known_frame_attributes:
                        values.append(encode_glotzformats_data(file_id, cache.unique_id, frame, attr))
                    all_values.append(values)

        for values in all_values:
            conn.execute('INSERT INTO glotzformats_frames VALUES '
                         '(?, ?, ?, ?, ?, ?)', values)

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
