import gtar
import json
import sqlite3
from .. import Cache, util

def open_gtar(cache_id, file_row):
    cache = Cache.get_opened_cache(cache_id)
    opened_file = cache.open_file(file_row, 'rb')
    gtar_traj = gtar.GTAR(opened_file.name, 'r')
    return (opened_file, gtar_traj)

def close_gtar(args):
    (opened_file, gtar_traj) = args
    gtar_traj.close()
    opened_file.close()

def encode_gtar_data(path, file_id, cache_id):
    return json.dumps([path, file_id, cache_id]).encode('UTF-8')

def convert_gtar_data(contents):
    (path, file_id, cache_id) = json.loads(contents.decode('UTF-8'))
    cache = Cache.get_opened_cache(cache_id)
    for row in cache.query('SELECT * from files WHERE rowid = ?', (file_id,)):
        # set row for open_file below
        pass

    (_, traj) = GTAR.opened_trajectories_(cache_id, row)
    return traj.readPath(path)

class GTAR:
    """Interpret getar-format files.

    `GTAR` parses zip, tar, and sqlite-format archives in the getar
    format (https://libgetar.readthedocs.io) to expose trajectory
    data. The getar files themselves are opened upon indexing to find
    which records are available in each file, but the actual data
    contents are read on-demand.

    GTAR objects create the following table in the database:

    - gtar_records: Contains links to data found in all getar-format files

    The **gtar_records** table has the following columns:

    - path: path within the archive of the record
    - gtar_group: *group* for the record
    - gtar_index: *index* for the record
    - name: *name* for the record
    - file_id: files table identifier for the archive containing this record
    - cache_id: `Cache` unique identifier for the archive containing this record
    - data: exposes the data of the record. Value is a string, bytes, or array-like object depending on the stored format.

    .. note::
        Consult the libgetar documentation to find more details about
        how records are encoded.

    """
    opened_trajectories_ = util.LRU_Cache(open_gtar, close_gtar, 16)

    def __init__(self):
        pass

    def index(self, cache, conn, mine_id=None, force=False):
        self.check_adapters()

        conn.execute('CREATE TABLE IF NOT EXISTS gtar_records '
                     '(path TEXT, gtar_group TEXT, gtar_index TEXT, name TEXT, '
                     'file_id INTEGER, cache_id TEXT, data GTAR_DATA, '
                     'CONSTRAINT unique_gtar_path '
                     'UNIQUE (path, file_id, cache_id) ON CONFLICT IGNORE)')

        # don't do file IO if we aren't forced
        if not force:
            return

        # all rows to insert into glotzformats_frames (TODO interleave
        # reading and writing if size of all_values becomes an issue)
        all_values = []
        for row in conn.execute(
                'SELECT rowid, * from files WHERE path LIKE "%.zip" OR '
                'path LIKE "%.tar" OR path LIKE "%.sqlite"'):
            file_id = row[0]
            row = row[1:]

            (_, traj) = GTAR.opened_trajectories_(cache.unique_id, row)
            for record in traj.getRecordTypes():
                group = record.getGroup()
                name = record.getName()
                for frame in traj.queryFrames(record):
                    record.setIndex(frame)
                    path = record.getPath()

                    encoded_data = encode_gtar_data(
                        path, file_id, cache.unique_id)
                    values = (path, group, frame, name, file_id,
                              cache.unique_id, encoded_data)
                    all_values.append(values)

        for values in all_values:
            conn.execute(
                'INSERT INTO gtar_records VALUES (?, ?, ?, ?, ?, ?, ?)', values)

    @classmethod
    def check_adapters(cls):
        try:
            if cls.has_registered_adapters:
                return
        except AttributeError:
            # hasn't been registered yet, run the rest of this function
            pass

        sqlite3.register_converter('GTAR_DATA', convert_gtar_data)
        cls.has_registered_adapters = True

    def __getstate__(self):
        return []

    def __setstate__(self, state):
        self.__init__(*state)
